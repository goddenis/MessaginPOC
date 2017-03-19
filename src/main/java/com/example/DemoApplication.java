package com.example;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.store.AbstractMessageGroupStore;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.messaging.*;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.UUID;


@SpringBootApplication
@IntegrationComponentScan
public class DemoApplication {

	private static SimpleMessageStore messageGroups =  new SimpleMessageStore();


	public static void main(String[] args) {

		ConfigurableApplicationContext ctx = SpringApplication.run(DemoApplication.class, args);
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		MyGateway gateway = ctx.getBean(MyGateway.class);
		UUID id = UUID.randomUUID();
		System.out.println(id.toString());
		gateway.sendToRabbit(id,"foo", id.toString());

//		messageGroups.removeMessage(id);

		messageGroups
				.getMessageGroup("silver")
				.getMessages()
				.stream()
				.filter(message -> message.getHeaders().get("id2").equals(id.toString()))
				.findAny()
				.ifPresent(message -> messageGroups.getMessageGroup("silver").remove(message));

		System.out.println(messageGroups.getMessageGroup("silver").getMessages());
	}

	@Bean
	public IntegrationFlow outBound(AmqpTemplate template){
		return IntegrationFlows.from(amqpOutboundChannel())
				.delay("silver",c->{
					c.defaultDelay(5000);
					c.messageStore(silverMessageStore());

				})
				.handle(Amqp.outboundAdapter(template).routingKey("silver"),c->{
					c.requiresReply(false);
//					c.async(true);
					c.autoStartup(true);

				})
				.get();

	}


	@Bean
	AbstractMessageGroupStore silverMessageStore(){
		return messageGroups;
	}

	@Bean
	AbstractMessageGroupStore otherMessageStore(){
		return new SimpleMessageStore();
	}
	@MessagingGateway(defaultRequestChannel = "amqpOutboundChannel" )
	public interface MyGateway {

		void sendToRabbit(@Header(MessageHeaders.ID) UUID id,@Payload String data,@Header("id2") String id2);

	}

	@Bean
	public MessageChannel amqpOutboundChannel() {

		return new DirectChannel();
	}


	@Bean
	public MessageChannel amqpInputChannel() {
		return new DirectChannel();
	}

	@Bean
	public AmqpInboundChannelAdapter inbound(SimpleMessageListenerContainer listenerContainer,
											 @Qualifier("amqpInputChannel") MessageChannel channel) {
		AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
		adapter.setOutputChannel(channel);
		return adapter;
	}

	@Bean
	public SimpleMessageListenerContainer container() {
		ConnectionFactory connectionFactory = new CachingConnectionFactory("localhost",5672);

		SimpleMessageListenerContainer container =
				new SimpleMessageListenerContainer( connectionFactory);
		container.setQueueNames("silver");
		container.setConcurrentConsumers(2);
		return container;
	}

	@Bean
	@ServiceActivator(inputChannel = "amqpInputChannel")
	public MessageHandler handler() {
		return new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				System.out.println(messageGroups.getMessageCount());
				System.out.println(message.getPayload());
			}

		};
	}


}
