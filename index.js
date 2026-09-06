"use strict";

const utils = require("./utils");
const log = require("npmlog");
const { MQTT } = require("./src/protocol");

let checkVerified = null;

const defaultLogRecordSize = 100;
log.maxRecordSize = defaultLogRecordSize;

function setOptions(globalOptions, options) {
	options = options || {};
	Object.keys(options).map(function (key) {
		switch (key) {
			case 'online':
				globalOptions.online = Boolean(options.online);
				break;
			case 'logLevel':
				log.level = options.logLevel;
				globalOptions.logLevel = options.logLevel;
				break;
			case 'logRecordSize':
				log.maxRecordSize = options.logRecordSize;
				globalOptions.logRecordSize = options.logRecordSize;
				break;
			case 'selfListen':
				globalOptions.selfListen = Boolean(options.selfListen);
				break;
			case 'selfListenEvent':
				globalOptions.selfListenEvent = options.selfListenEvent;
				break;
			case 'listenEvents':
				globalOptions.listenEvents = Boolean(options.listenEvents);
				break;
			case 'pageID':
				globalOptions.pageID = options.pageID == null ? undefined : options.pageID.toString();
				break;
			case 'updatePresence':
				globalOptions.updatePresence = Boolean(options.updatePresence);
				break;
			case 'forceLogin':
				globalOptions.forceLogin = Boolean(options.forceLogin);
				break;
			case 'userAgent':
				globalOptions.userAgent = options.userAgent;
				break;
			case 'autoMarkDelivery':
				globalOptions.autoMarkDelivery = Boolean(options.autoMarkDelivery);
				break;
			case 'autoMarkRead':
				globalOptions.autoMarkRead = Boolean(options.autoMarkRead);
				break;
			case 'listenTyping':
				globalOptions.listenTyping = Boolean(options.listenTyping);
				break;
			case 'proxy':
				if (typeof options.proxy != "string") {
					delete globalOptions.proxy;
					utils.setProxy();
				} else {
					globalOptions.proxy = options.proxy;
					utils.setProxy(globalOptions.proxy);
				}
				break;
			case 'autoReconnect':
				globalOptions.autoReconnect = Boolean(options.autoReconnect);
				break;
			case 'emitReady':
				globalOptions.emitReady = Boolean(options.emitReady);
				break;
			default:
				log.warn("setOptions", "Unrecognized option given to setOptions: " + key);
				break;
		}
	});
}

function buildAPI(globalOptions, html, jar) {
	const getCookies = typeof jar.getCookiesSync === "function" ? jar.getCookiesSync.bind(jar) : jar.getCookies.bind(jar);
	const maybeCookie = getCookies("https://www.facebook.com").filter(function (val) {
		return val.cookieString().split("=")[0] === "c_user";
	});

	const objCookie = getCookies("https://www.facebook.com").reduce(function (obj, val) {
		obj[val.cookieString().split("=")[0]] = val.cookieString().split("=")[1];
		return obj;
	}, {});

	if (maybeCookie.length === 0) {
		throw { error: "Error retrieving userID. This can be caused by a lot of things, including getting blocked by Facebook for logging in from an unknown location. Try logging in with a browser to verify." };
	}

	if (html.indexOf("/checkpoint/block/?next") > -1) {
		log.warn("login", "Checkpoint detected. Please log in with a browser to verify.");
	}

	const userID = maybeCookie[0].cookieString().split("=")[1].toString();
	const i_userID = objCookie.i_user || null;
	log.info("login", `Logged in as ${userID}`);

	try {
		clearInterval(checkVerified);
	} catch (_) { }

	const clientID = (Math.random() * 2147483648 | 0).toString(16);


	const oldFBMQTTMatch = html.match(new RegExp(`irisSeqID:"([^"]+)",appID:${MQTT.appId},endpoint:"([^"]+)"`));
	let mqttEndpoint = null;
	let region = null;
	let irisSeqID = null;
	let noMqttData = null;

	const setMqttEndpoint = function (endpoint) {
		mqttEndpoint = endpoint && endpoint.replace(/\\\//g, "/");
		try {
			region = new URL(mqttEndpoint).searchParams.get("region");
			region = region ? region.toUpperCase() : null;
		}
		catch (_) {
			region = null;
		}
	};

	if (oldFBMQTTMatch) {
		irisSeqID = oldFBMQTTMatch[1];
		setMqttEndpoint(oldFBMQTTMatch[2]);
		log.info("login", `Got this account's message region: ${region}`);
	} else {
		const newFBMQTTMatch = html.match(new RegExp(`\\{"app_id":"${MQTT.appId}","endpoint":"([^"]+)","iris_seq_id":"([^"]+)"\\}`));
		if (newFBMQTTMatch) {
			irisSeqID = newFBMQTTMatch[2];
			setMqttEndpoint(newFBMQTTMatch[1]);
			log.info("login", `Got this account's message region: ${region}`);
		} else {
			const legacyFBMQTTMatch = html.match(new RegExp(`(\\["MqttWebConfig",\\[\\],\\{fbid:")(.+?)(",appID:${MQTT.appId},endpoint:")(.+?)(",pollingEndpoint:")(.+?)(3790])`));
			if (legacyFBMQTTMatch) {
				setMqttEndpoint(legacyFBMQTTMatch[4]);
				log.warn("login", `Cannot get sequence ID with new RegExp. Fallback to old RegExp (without seqID)...`);
				log.info("login", `Got this account's message region: ${region}`);
				log.info("login", `[Unused] Polling endpoint: ${legacyFBMQTTMatch[6]}`);
			} else {
				log.warn("login", "Cannot get MQTT region & sequence ID.");
				noMqttData = html;
			}
		}
	}

	// All data available to api functions
	const ctx = {
		userID: userID,
		i_userID: i_userID,
		jar: jar,
		clientID: clientID,
		globalOptions: globalOptions,
		loggedIn: true,
		access_token: 'NONE',
		clientMutationId: 0,
		mqttClient: undefined,
		// The sequence embedded in page HTML is stale on some sessions. The
		// realtime transport fetches a fresh sequence through GraphQL instead.
		lastSeqId: null,
		initialIrisSeqId: irisSeqID,
		syncToken: undefined,
		mqttEndpoint,
		region,
		firstListen: true
	};

	const api = {
		setOptions: setOptions.bind(null, globalOptions),
		getAppState: function getAppState() {
			const appState = utils.getAppState(jar);
			// filter duplicate
			return appState.filter((item, index, self) => self.findIndex((t) => { return t.key === item.key }) === index);
		}
	};

	if (noMqttData) {
		api["htmlData"] = noMqttData;
	}

	const apiFuncNames = [
		'addExternalModule',
		'addUserToGroup',
		'changeAdminStatus',
		'changeArchivedStatus',
		'changeAvatar',
		'changeBio',
		'changeBlockedStatus',
		'changeGroupImage',
		'changeNickname',
		'changeThreadColor',
		'changeThreadEmoji',
		'createAiTheme',
		'createNewGroup',
		'createPoll',
		'deleteMessage',
		'deleteThread',
		'editMessage',
		'forwardAttachment',
		'getCurrentUserID',
		'getEmojiUrl',
		'getFriendsList',
		'getMessage',
		'getThreadHistory',
		'getThreadInfo',
		'getThreadList',
		'getThreadPictures',
		'getThemePictures',
		'getUserID',
		'getUserInfo',
		'handleMessageRequest',
		'listenMqtt',
		'logout',
		'markAsDelivered',
		'markAsRead',
		'markAsReadAll',
		'markAsSeen',
		'muteThread',
		'refreshFb_dtsg',
		'removeUserFromGroup',
		'resolvePhotoUrl',
		'searchForThread',
		'sendMessage',
		'sendMessageMqtt',
		'sendTypingIndicator',
		'shareContact',
		'setMessageReaction',
		'setPostReaction',
		'setTitle',
		'threadColors',
		'unsendMessage',
		'unfriend',

		// HTTP
		'httpGet',
		'httpPost',
		'httpPostFormData',

		'uploadAttachment'
	];

	const defaultFuncs = utils.makeDefaults(html, i_userID || userID, ctx);

	// Load all api functions in a loop
	apiFuncNames.map(function (v) {
		api[v] = require('./src/' + v)(defaultFuncs, api, ctx);
	});
	api.getThreadHistoryGraphQL = api.getThreadHistory;
	api.createAITheme = api.createAiTheme;
	api.createThemeAI = api.createAiTheme;
	const httpSendMessage = api.sendMessage;
	api.sendMessage = function sendMessage(message, threadID, callback, replyToMessage, isGroup) {
		const hasUrl = message && typeof message === "object" && message.url;
		if (ctx.mqttClient && !hasUrl && !Array.isArray(threadID)) {
			return api.sendMessageMqtt(message, threadID, callback, replyToMessage);
		}
		return httpSendMessage(message, threadID, callback, replyToMessage, isGroup);
	};

	//Removing original `listen` that uses pull.
	//Map it to listenMqtt instead for backward compatibly.
	api.listen = api.listenMqtt;

	return [ctx, defaultFuncs, api];
}

// Helps the login
function loginHelper(appState, email, password, globalOptions, callback, prCallback) {
	let mainPromise = null;
	const jar = utils.getJar();

	// If we're given an appState we loop through it and save each cookie
	// back into the jar.
	if (appState) {
		// check and convert cookie to appState
		if (utils.getType(appState) === 'Array' && appState.some(c => c.name)) {
			appState = appState.map(c => {
				c.key = c.name;
				delete c.name;
				return c;
			})
		}
		else if (utils.getType(appState) === 'String') {
			const arrayAppState = [];
			appState.split(';').forEach(c => {
				const separator = c.indexOf('=');
				if (separator < 1) return;
				const key = c.slice(0, separator);
				const value = c.slice(separator + 1);

				arrayAppState.push({
					key: (key || "").trim(),
					value: (value || "").trim(),
					domain: "facebook.com",
					path: "/",
					expires: new Date(Date.now() + 1000 * 60 * 60 * 24 * 365).toUTCString()
				});
			});
			appState = arrayAppState;
		}

		appState.map(function (c) {
			const domain = String(c.domain || "facebook.com").replace(/^\./, "");
			const expiresValue = c.expires instanceof Date ? c.expires.toUTCString() :
				typeof c.expires === "number" ? new Date(c.expires).toUTCString() : c.expires;
			const expires = expiresValue && expiresValue !== "Infinity" ? "; expires=" + expiresValue : "";
			const str = c.key + "=" + c.value + expires + "; domain=." + domain + "; path=" + (c.path || "/") + ";";
			jar.setCookieSync(str, "https://www." + domain.replace(/^www\./, ""));
		});

		// Load the main page.
		mainPromise = utils
			.get('https://www.facebook.com/', jar, null, globalOptions, { noRef: true })
			.then(utils.saveCookies(jar));
	} else {
		if (email) {
			throw { error: "Currently, the login method by email and password is no longer supported, please use the login method by appState" };
		}
		else {
			throw { error: "No appState given." };
		}
	}

	let ctx = null;
	let _defaultFuncs = null;
	let api = null;

	mainPromise = mainPromise
		.then(function (res) {
			// Hacky check for the redirection that happens on some ISPs, which doesn't return statusCode 3xx
			const reg = /<meta http-equiv="refresh" content="0;url=([^"]+)[^>]+>/;
			const redirect = reg.exec(res.body);
			if (redirect && redirect[1]) {
				return utils
					.get(redirect[1], jar, null, globalOptions)
					.then(utils.saveCookies(jar));
			}
			return res;
		})
		.then(function (res) {
			const html = res.body;
			const stuff = buildAPI(globalOptions, html, jar);
			ctx = stuff[0];
			_defaultFuncs = stuff[1];
			api = stuff[2];
			return res;
		});

	// given a pageID we log in as a page
	if (globalOptions.pageID) {
		mainPromise = mainPromise
			.then(function () {
				return utils
					.get('https://www.facebook.com/' + ctx.globalOptions.pageID + '/messages/?section=messages&subsection=inbox', ctx.jar, null, globalOptions);
			})
			.then(function (resData) {
				let url = utils.getFrom(resData.body, 'window.location.replace("https:\\/\\/www.facebook.com\\', '");').split('\\').join('');
				url = url.substring(0, url.length - 1);

				return utils
					.get('https://www.facebook.com' + url, ctx.jar, null, globalOptions);
			});
	}

	// At the end we call the callback or catch an exception
	mainPromise
		.then(function () {
			log.info("login", 'Done logging in.');
			return callback(null, api);
		})
		.catch(function (e) {
			log.error("login", e.error || e);
			callback(e);
		});
}

function login(loginData, options, callback) {
	if (utils.getType(options) === 'Function' || utils.getType(options) === 'AsyncFunction') {
		callback = options;
		options = {};
	}
	options = options || {};
	loginData = loginData || {};

	const globalOptions = {
		selfListen: false,
		selfListenEvent: false,
		listenEvents: false,
		listenTyping: false,
		updatePresence: false,
		forceLogin: false,
		autoMarkDelivery: true,
		autoMarkRead: false,
		autoReconnect: true,
		logRecordSize: defaultLogRecordSize,
		online: true,
		emitReady: false,
		userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/600.3.18 (KHTML, like Gecko) Version/8.0.3 Safari/600.3.18"
	};

	setOptions(globalOptions, options);

	let prCallback = null;
	if (utils.getType(callback) !== "Function" && utils.getType(callback) !== "AsyncFunction") {
		let rejectFunc = null;
		let resolveFunc = null;
		var returnPromise = new Promise(function (resolve, reject) {
			resolveFunc = resolve;
			rejectFunc = reject;
		});
		prCallback = function (error, api) {
			if (error) {
				return rejectFunc(error);
			}
			return resolveFunc(api);
		};
		callback = prCallback;
	}
	loginHelper(loginData.appState, loginData.email, loginData.password, globalOptions, callback, prCallback);
	return returnPromise;
}

module.exports = login;
