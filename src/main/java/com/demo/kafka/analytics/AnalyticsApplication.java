package com.demo.kafka.analytics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
@EnableAutoConfiguration
@Slf4j
public class AnalyticsApplication {



	@Component
	public static class PageViewEventSource {

		private final AnalyticsBinding analyticsBinding;



		public PageViewEventSource(AnalyticsBinding analyticsBinding) {
			this.analyticsBinding = analyticsBinding;
		}


		public void sendAnalysis(PageViewEvent pageViewEvent) throws Exception {


			MessageChannel messageChannel =  analyticsBinding.pageViewsOut();
			messageChannel.send(MessageBuilder
					.withPayload(pageViewEvent)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
					.build());
			log.info("Message Sent Successfully : {} ", pageViewEvent);



		}
	}

	@RestController
	public static class GreetingController{

		private final PageViewEventSource pageViewEventSource;

		public GreetingController(PageViewEventSource pageViewEventSource) {
			this.pageViewEventSource = pageViewEventSource;
		}

		@GetMapping("/analysis")
		@ResponseStatus(HttpStatus.ACCEPTED)
		public void greetings(@RequestParam("analysis") String message) throws Exception {
			PageViewEvent analysis = PageViewEvent.builder()
					.message(message)
					.timestamp(System.currentTimeMillis())
					.build();

			pageViewEventSource.sendAnalysis(analysis);
		}
	}

	@Component
	public static class PageViewEventProcessor{



		@StreamListener(AnalyticsBinding.PAGE_VIEWS_IN)
		public void process(
				@Payload PageViewEvent events ){



			log.info("Successfully reached the consumer as it is {} : ",events);




		}
	}

	public static void main(String[] args) {
		SpringApplication.run(AnalyticsApplication.class, args);
	}

}

interface AnalyticsBinding{

	String PAGE_VIEWS_OUT = "pvout";

	String PAGE_VIEWS_IN = "pvin";


	@Input(PAGE_VIEWS_IN)
	SubscribableChannel pageViewsIn();


	@Output(PAGE_VIEWS_OUT)
	MessageChannel pageViewsOut();
}


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
class PageViewEvent{

	private long timestamp;
	private String message;
}