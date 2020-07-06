package com.zendesk.maxwell;

import joptsimple.OptionSet;
import org.threeten.bp.Duration;

import java.util.Properties;

public class PubSubConfig extends MaxwellConfig {

	public String pubsubProjectId;
	public String pubsubTopic;
	public String ddlPubsubTopic;
	public Long pubsubRequestBytesThreshold;
	public Long pubsubMessageCountBatchSize;
	public Duration pubsubPublishDelayThreshold;
	public Duration pubsubRetryDelay;
	public Double pubsubRetryDelayMultiplier;
	public Duration pubsubMaxRetryDelay;
	public Duration pubsubInitialRpcTimeout;
	public Double pubsubRpcTimeoutMultiplier;
	public Duration pubsubMaxRpcTimeout;
	public Duration pubsubTotalTimeout;


	protected void setup(OptionSet options, Properties properties) {
		super.setup(options, properties);

		this.pubsubProjectId = fetchOption("pubsub_project_id", options, properties, null);
		this.pubsubTopic = fetchOption("pubsub_topic", options, properties, "maxwell");
		this.ddlPubsubTopic = fetchOption("ddl_pubsub_topic", options, properties, this.pubsubTopic);
		this.pubsubRequestBytesThreshold = fetchLongOption("pubsub_request_bytes_threshold", options, properties, 1L);
		this.pubsubMessageCountBatchSize = fetchLongOption("pubsub_message_count_batch_size", options, properties, 1L);
		this.pubsubPublishDelayThreshold = Duration.ofMillis(fetchLongOption("pubsub_publish_delay_threshold", options, properties, 1L));
		this.pubsubRetryDelay = Duration.ofMillis(fetchLongOption("pubsub_retry_delay", options, properties, 100L));
		this.pubsubRetryDelayMultiplier = Double.parseDouble(fetchOption("pubsub_retry_delay_multiplier", options, properties, "1.3"));
		this.pubsubMaxRetryDelay = Duration.ofSeconds(fetchLongOption("pubsub_max_retry_delay", options, properties, 60L));
		this.pubsubInitialRpcTimeout = Duration.ofSeconds(fetchLongOption("pubsub_initial_rpc_timeout", options, properties, 5L));
		this.pubsubRpcTimeoutMultiplier = Double.parseDouble(fetchOption("pubsub_rpc_timeout_multiplier", options, properties, "1.0"));
		this.pubsubMaxRpcTimeout = Duration.ofSeconds(fetchLongOption("pubsub_max_rpc_timeout", options, properties, 600L));
		this.pubsubTotalTimeout = Duration.ofSeconds(fetchLongOption("pubsub_total_timeout", options, properties, 600L));
	}

	@Override
	public void validate() {
		super.validate();
	    if (this.producerType.equals("pubsub")) {
			if (this.pubsubRequestBytesThreshold <= 0L)
				usage("--pubsub_request_bytes_threshold must be > 0");
			if (this.pubsubMessageCountBatchSize <= 0L)
				usage("--pubsub_message_count_batch_size must be > 0");
			if (this.pubsubPublishDelayThreshold.isNegative() || this.pubsubPublishDelayThreshold.isZero())
				usage("--pubsub_publish_delay_threshold must be > 0");
			if (this.pubsubRetryDelay.isNegative() || this.pubsubRetryDelay.isZero())
				usage("--pubsub_retry_delay must be > 0");
			if (this.pubsubRetryDelayMultiplier <= 1.0)
				usage("--pubsub_retry_delay_multiplier must be > 1.0");
			if (this.pubsubMaxRetryDelay.isNegative() || this.pubsubMaxRetryDelay.isZero())
				usage("--pubsub_max_retry_delay must be > 0");
			if (this.pubsubInitialRpcTimeout.isNegative() || this.pubsubInitialRpcTimeout.isZero())
				usage("--pubsub_initial_rpc_timeout must be > 0");
			if (this.pubsubRpcTimeoutMultiplier < 1.0)
				usage("--pubsub_rpc_timeout_multiplier must be >= 1.0");
			if (this.pubsubMaxRpcTimeout.isNegative() || this.pubsubMaxRpcTimeout.isZero())
				usage("--pubsub_max_rpc_timeout must be > 0");
			if (this.pubsubTotalTimeout.isNegative() || this.pubsubTotalTimeout.isZero())
				usage("--pubsub_total_timeout must be > 0");
		}
	}
}
