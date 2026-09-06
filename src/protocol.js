"use strict";

// Facebook changes these persisted-query and Lightspeed identifiers without
// changing the public API. Keep them in one place so protocol refreshes do not
// require hunting through every feature module.
const GRAPHQL_DOCS = Object.freeze({
	threadList: "3336396659757871",
	threadInfo: "3449967031715030",
	threadHistory: "1498317363570230",
	message: "1768656253222505",
	forcedFetch: "2848441488556444",
	createGroup: "577041672419534",
	changeThreadColor: "1727493033983591",
	changeBio: "2725043627607610",
	changeAvatar: "5066134240065849",
	messageReaction: "1491398900900362",
	postReaction: "4769042373179384",
	aiTheme: "23873748445608673",
	themePictures: "9734829906576883"
});

const MQTT = Object.freeze({
	appId: "219994525426954",
	lightspeedAppId: "2220391788200892",
	syncApiVersion: 11,
	maxDeltas: 100,
	deltaBatchSize: 500,
	keepalive: 30,
	topics: Object.freeze([
		"/ls_req",
		"/ls_resp",
		"/legacy_web",
		"/webrtc",
		"/rtc_multi",
		"/onevc",
		"/br_sr",
		"/sr_res",
		"/t_ms",
		"/thread_typing",
		"/orca_typing_notifications",
		"/notify_disconnect",
		"/orca_presence",
		"/inbox",
		"/mercury",
		"/messaging_events",
		"/orca_message_notifications",
		"/pp",
		"/webrtc_response"
	]),
	messageVersion: "24804310205905615",
	threadMutationVersion: "8798795233522156",
	contactShareVersion: "7214102258676893",
	editMessageVersion: "6903494529735864"
});

module.exports = { GRAPHQL_DOCS, MQTT };
