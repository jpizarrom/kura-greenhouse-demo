/**
 * Copyright (c) 2014 Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Benjamin Cabé, Eclipse Foundation
 */
package org.eclipse.iot.greenhouse.publisher;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.eclipse.iot.greenhouse.sensors.SensorService;
import org.eclipse.iot.greenhouse.sensors.SensorService.NoSuchSensorOrActuatorException;
import org.eclipse.iot.greenhouse.sensors.SensorChangedListener;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraNotConnectedException;
import org.eclipse.kura.KuraTimeoutException;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.data.DataServiceListener;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreenhousePublisher implements ConfigurableComponent,
		DataServiceListener, SensorChangedListener {
	private static final Logger s_logger = LoggerFactory
			.getLogger(GreenhousePublisher.class);

	private static final String PUBLISH_TOPICPREFIX_PROP_NAME = "publish.appTopicPrefix";
	private static final String PUBLISH_QOS_PROP_NAME = "publish.qos";
	private static final String PUBLISH_RETAIN_PROP_NAME = "publish.retain";
	
	private static final String PUBLISH_M2X_API_KEY = "publish.m2xApiKey";
	private static final String PUBLISH_M2X_DEVICE = "publish.m2xDeviceID";

	private DataService _dataService;
	private SensorService _sensorService;

	private Map<String, Object> _properties;

	// ----------------------------------------------------------------
	//
	// Dependencies
	//
	// ----------------------------------------------------------------

	public GreenhousePublisher() {
		super();
	}

	protected void setGreenhouseSensorService(
			SensorService sensorService) {
		_sensorService = sensorService;
	}

	protected void unsetGreenhouseSensorService(
			SensorService sensorService) {
		_sensorService = null;
	}

	public void setDataService(DataService dataService) {
		_dataService = dataService;
	}

	public void unsetDataService(DataService dataService) {
		_dataService = null;
	}

	// ----------------------------------------------------------------
	//
	// Activation APIs
	//
	// ----------------------------------------------------------------

	protected void activate(ComponentContext componentContext,
			Map<String, Object> properties) {
		_properties = properties;
		s_logger.info("Activating GreenhousePublisher... Done.");
	}

	protected void deactivate(ComponentContext componentContext) {
		s_logger.debug("Deactivating GreenhousePublisher... Done.");
	}

	public void updated(Map<String, Object> properties) {
		_properties = properties;
	}

	@Override
	public void onConnectionEstablished() {
		try {
			String topic = "m2x/"+(String) _properties.get(PUBLISH_M2X_API_KEY)+"/commands";
			
			_dataService.subscribe(topic, 0);
		} catch (KuraTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KuraNotConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KuraException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void onDisconnecting() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDisconnected() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionLost(Throwable cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageArrived(String topic, byte[] payload, int qos,
			boolean retained) {
//		String prefix = (String) _properties.get(PUBLISH_TOPICPREFIX_PROP_NAME);
		String prefix = "m2x";
		s_logger.info("onMessageArrived to {} message: {}",
				new Object[] { topic, new String(payload) });

		if (!topic.startsWith(prefix)) {
			return;
		}

		String[] topicFragments = topic.split("/");
//		// topicFragments[0] == {appSetting.topic_prefix}
//		// topicFragments[1] == {M2X API Key}
//		// topicFragments[2] == "commands"

		if (topicFragments.length != 3)
			return;

		if (topicFragments[2].equals("commands")) {
			try {
				_sensorService.setActuatorValue("commands", new String(
						payload));
			} catch (NoSuchSensorOrActuatorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void onMessagePublished(int messageId, String topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageConfirmed(int messageId, String topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void sensorChanged(String sensorName, Object newValue) {
		if (_dataService == null) {
			s_logger.warn("failed to publish message, data service is not ready");
	        return;
	    }
		// Publish the message
		String prefix = (String) _properties.get(PUBLISH_TOPICPREFIX_PROP_NAME);
		Integer qos = (Integer) _properties.get(PUBLISH_QOS_PROP_NAME);
		Boolean retain = (Boolean) _properties.get(PUBLISH_RETAIN_PROP_NAME);

		String topic = prefix + "sensors/" + sensorName;
		String resource = "/v2/devices/"+(String) _properties.get(PUBLISH_M2X_DEVICE)+"/streams/"+sensorName+"/values";
		String payload = newValue.toString();
//		temperature
//		relativehumidity
//		moisture
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // 2016-02-07T12:08:30.537121Z
		topic = "m2x/"+(String) _properties.get(PUBLISH_M2X_API_KEY)+"/requests";
		payload = "{"+
		          "  \"id\": \""+ UUID.randomUUID() +"\"," +
		          "  \"method\": \"POST\","+
		          "  \"resource\": \""+resource+"\","+
		          "  \"agent\": \"M2X-Demo-Client/0.0.1\","+
		          "  \"body\": {"+
		          "    \"values\": ["+
		          "      { "+
		          "      \"timestamp\": \""+ df.format(new Date()) +"\","+
		          "      \"value\": " + newValue +
		          "      }"+
		          "    ]"+
		          "  }"+
		          "}";

		try {

			int messageId = _dataService.publish(topic, payload.getBytes(), qos, retain, 2);
			s_logger.info("Published to {} message: {} with ID: {}",
					new Object[] { topic, payload, messageId });
		} catch (Exception e) {
			s_logger.error("Cannot publish topic: " + topic, e);
		}
	}
}
