/**
 * Deriv Digit Trading Bot - Main Server
 * OAuth 2.0 + PKCE Authentication | WebSocket Manager | Trading Engine
 * Version 2.0.0
 */

const express = require('express');
const session = require('express-session');
const crypto = require('crypto');
const WebSocket = require('ws');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================
// CONFIGURATION
// ============================================================
const CONFIG = {
    DERIV_APP_ID: process.env.DERIV_APP_ID || '33QtvG4sVV4bjhAR2dqyL',
    DERIV_REDIRECT_URI: process.env.DERIV_REDIRECT_URI || 'https://digitaibot.onrender.com/auth/deriv/callback',
    DERIV_OAUTH_URL: process.env.DERIV_OAUTH_URL || 'https://oauth.deriv.com',
    DERIV_API_URL: process.env.DERIV_API_URL || 'wss://ws.derivws.com/websockets/v3',
    DERIV_TOKEN_URL: 'https://auth.deriv.com/oauth2/token',
    DERIV_AUTHORIZE_URL: 'https://auth.deriv.com/oauth2/auth',
    SESSION_SECRET: process.env.SESSION_SECRET || 'deriv-bot-secret-' + crypto.randomBytes(32).toString('hex'),
};

// ============================================================
// MIDDLEWARE
// ============================================================
app.use(cors({
    origin: true,
    credentials: true
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(session({
    secret: CONFIG.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
        secure: process.env.NODE_ENV === 'production',
        httpOnly: true,
        maxAge: 24 * 60 * 60 * 1000, // 24 hours
        sameSite: 'lax'
    },
    name: 'derivBotSession'
}));

app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// PKCE & OAUTH HELPERS
// ============================================================

/**
 * Generate a cryptographically secure code_verifier for PKCE
 * RFC 7636: 43-128 characters of [A-Z] / [a-z] / [0-9] / "-" / "." / "_" / "~"
 */
function generateCodeVerifier() {
    const length = 128;
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~';
    let verifier = '';
    const randomBytes = crypto.randomBytes(length);
    for (let i = 0; i < length; i++) {
        verifier += possible[randomBytes[i] % possible.length];
    }
    return verifier;
}

/**
 * Generate code_challenge from code_verifier using S256
 * Base64URL encode the SHA256 hash of the verifier
 */
function generateCodeChallenge(verifier) {
    const hash = crypto.createHash('sha256').update(verifier).digest();
    return hash.toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=/g, '');
}

/**
 * Generate a random state parameter for CSRF protection
 */
function generateState() {
    return crypto.randomBytes(32).toString('hex');
}

// ============================================================
// IN-MEMORY STORES (Production: use Redis)
// ============================================================
const pkceStore = new Map();      // state -> { code_verifier, code_challenge, createdAt }
const sessionStore = new Map();   // sessionId -> user data
const wsConnections = new Map();  // sessionId -> WebSocket
const botStates = new Map();      // sessionId -> bot state

// Cleanup old PKCE entries every 10 minutes
setInterval(() => {
    const now = Date.now();
    const expiry = 10 * 60 * 1000; // 10 minutes
    for (const [state, data] of pkceStore.entries()) {
        if (now - data.createdAt > expiry) {
            pkceStore.delete(state);
        }
    }
}, 10 * 60 * 1000);

// ============================================================
// DERIV WEBSOCKET MANAGER
// ============================================================

class DerivWebSocketManager {
    constructor(sessionId) {
        this.sessionId = sessionId;
        this.ws = null;
        this.isConnected = false;
        this.isAuthorized = false;
        this.authorizedAccount = null;
        this.messageQueue = [];
        this.subscriptions = new Map();
        this.reqId = 1;
        this.pingInterval = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 1000;
        this.pendingRequests = new Map();
        this.tickBuffers = new Map(); // symbol -> tick array
        this.accountInfo = null;
        this.balance = null;
    }

    connect() {
        return new Promise((resolve, reject) => {
            try {
                this.ws = new WebSocket(CONFIG.DERIV_API_URL);

                this.ws.on('open', () => {
                    console.log(`[WS ${this.sessionId}] Connected to Deriv`);
                    this.isConnected = true;
                    this.reconnectAttempts = 0;
                    this.reconnectDelay = 1000;

                    // Start ping interval
                    this.pingInterval = setInterval(() => {
                        if (this.isConnected) {
                            this.send({ ping: 1 });
                        }
                    }, 30000);

                    // Process queued messages
                    while (this.messageQueue.length > 0) {
                        const msg = this.messageQueue.shift();
                        this.ws.send(JSON.stringify(msg));
                    }

                    resolve(true);
                });

                this.ws.on('message', (data) => {
                    try {
                        const response = JSON.parse(data.toString());
                        this.handleMessage(response);
                    } catch (e) {
                        console.error(`[WS ${this.sessionId}] Parse error:`, e.message);
                    }
                });

                this.ws.on('close', () => {
                    console.log(`[WS ${this.sessionId}] Connection closed`);
                    this.cleanup();
                    this.attemptReconnect();
                });

                this.ws.on('error', (err) => {
                    console.error(`[WS ${this.sessionId}] Error:`, err.message);
                    reject(err);
                });

            } catch (err) {
                reject(err);
            }
        });
    }

    cleanup() {
        this.isConnected = false;
        this.isAuthorized = false;
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
    }

    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[WS ${this.sessionId}] Max reconnect attempts reached`);
            return;
        }

        this.reconnectAttempts++;
        const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), 30000);

        console.log(`[WS ${this.sessionId}] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);

        setTimeout(() => {
            this.connect().then(() => {
                // Re-authorize if we have a token
                const sessionData = sessionStore.get(this.sessionId);
                if (sessionData && sessionData.accessToken) {
                    this.authorize(sessionData.accessToken);
                }
            }).catch(err => {
                console.error(`[WS ${this.sessionId}] Reconnect failed:`, err.message);
            });
        }, delay);
    }

    send(message) {
        if (this.isConnected && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        } else {
            this.messageQueue.push(message);
        }
    }

    sendWithResponse(message) {
        return new Promise((resolve, reject) => {
            const reqId = this.reqId++;
            message.req_id = reqId;
            this.pendingRequests.set(reqId, { resolve, reject, timeout: setTimeout(() => {
                this.pendingRequests.delete(reqId);
                reject(new Error('Request timeout'));
            }, 30000) });
            this.send(message);
        });
    }

    handleMessage(response) {
        // Handle req_id responses
        if (response.req_id !== undefined && this.pendingRequests.has(response.req_id)) {
            const pending = this.pendingRequests.get(response.req_id);
            clearTimeout(pending.timeout);
            this.pendingRequests.delete(response.req_id);

            if (response.error) {
                pending.reject(new Error(response.error.message));
            } else {
                pending.resolve(response);
            }
            return;
        }

        // Handle tick data
        if (response.tick) {
            this.handleTick(response.tick);
            return;
        }

        // Handle buy response
        if (response.buy) {
            this.emitToClient('trade_executed', response.buy);
            return;
        }

        // Handle proposal response
        if (response.proposal) {
            this.emitToClient('proposal_received', response.proposal);
            return;
        }

        // Handle balance updates
        if (response.balance) {
            this.balance = response.balance;
            this.emitToClient('balance_update', response.balance);
            return;
        }

        // Handle transaction (trade result)
        if (response.transaction) {
            this.emitToClient('transaction', response.transaction);
            return;
        }

        // Handle ping
        if (response.pong) {
            return;
        }

        // Log unhandled messages
        if (Object.keys(response).length > 0) {
            console.log(`[WS ${this.sessionId}] Unhandled:`, Object.keys(response));
        }
    }

    handleTick(tick) {
        const symbol = tick.symbol;
        if (!this.tickBuffers.has(symbol)) {
            this.tickBuffers.set(symbol, []);
        }
        const buffer = this.tickBuffers.get(symbol);
        buffer.push({
            price: tick.quote,
            digit: parseInt(tick.quote.toString().slice(-1)),
            timestamp: tick.epoch || Date.now() / 1000
        });
        // Keep last 1000 ticks
        if (buffer.length > 1000) {
            buffer.shift();
        }
        this.emitToClient('tick', { symbol, tick });
    }

    emitToClient(event, data) {
        // Broadcast to all connected dashboard clients for this session
        const clients = dashboardClients.get(this.sessionId);
        if (clients) {
            clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ event, data }));
                }
            });
        }
    }

    async authorize(token) {
        try {
            const response = await this.sendWithResponse({
                authorize: token
            });

            if (response.authorize) {
                this.isAuthorized = true;
                this.authorizedAccount = response.authorize;
                this.accountInfo = {
                    loginid: response.authorize.loginid,
                    email: response.authorize.email,
                    fullname: response.authorize.fullname,
                    currency: response.authorize.currency,
                    balance: response.authorize.balance,
                    is_virtual: response.authorize.is_virtual
                };

                // Store in session
                const sessionData = sessionStore.get(this.sessionId);
                if (sessionData) {
                    sessionData.accountInfo = this.accountInfo;
                    sessionData.isAuthorized = true;
                }

                // Subscribe to balance
                this.send({ balance: 1, subscribe: 1 });

                console.log(`[WS ${this.sessionId}] Authorized as ${this.accountInfo.loginid}`);
                this.emitToClient('authorized', this.accountInfo);

                return this.accountInfo;
            }
        } catch (err) {
            console.error(`[WS ${this.sessionId}] Authorization failed:`, err.message);
            this.isAuthorized = false;
            throw err;
        }
    }

    subscribeTicks(symbol) {
        this.send({
            ticks: symbol,
            subscribe: 1
        });
        this.subscriptions.set(symbol, 'ticks');
    }

    unsubscribeTicks(symbol) {
        this.send({
            ticks: symbol,
            subscribe: 0
        });
        this.subscriptions.delete(symbol);
    }

    async getProposal(params) {
        return this.sendWithResponse({
            proposal: 1,
            ...params
        });
    }

    async buyContract(proposalId, price) {
        return this.sendWithResponse({
            buy: proposalId,
            price: price
        });
    }

    async getBalance() {
        return this.sendWithResponse({ balance: 1 });
    }

    disconnect() {
        this.cleanup();
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        wsConnections.delete(this.sessionId);
    }
}

// ============================================================
// DASHBOARD WEBSOCKET (for frontend real-time updates)
// ============================================================
const dashboardWSS = new WebSocket.Server({ noServer: true });
const dashboardClients = new Map(); // sessionId -> Set of WebSocket clients

dashboardWSS.on('connection', (ws, req) => {
    const sessionId = req.sessionID;

    if (!dashboardClients.has(sessionId)) {
        dashboardClients.set(sessionId, new Set());
    }
    dashboardClients.get(sessionId).add(ws);

    console.log(`[Dashboard] Client connected for session ${sessionId}`);

    // Send current auth state
    const sessionData = sessionStore.get(sessionId);
    if (sessionData && sessionData.isAuthorized) {
        ws.send(JSON.stringify({
            event: 'auth_state',
            data: {
                isAuthenticated: true,
                accountInfo: sessionData.accountInfo
            }
        }));
    } else {
        ws.send(JSON.stringify({
            event: 'auth_state',
            data: { isAuthenticated: false }
        }));
    }

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            handleDashboardMessage(sessionId, msg, ws);
        } catch (e) {
            console.error('[Dashboard] Message parse error:', e.message);
        }
    });

    ws.on('close', () => {
        const clients = dashboardClients.get(sessionId);
        if (clients) {
            clients.delete(ws);
            if (clients.size === 0) {
                dashboardClients.delete(sessionId);
            }
        }
        console.log(`[Dashboard] Client disconnected for session ${sessionId}`);
    });
});

function handleDashboardMessage(sessionId, msg, ws) {
    switch (msg.action) {
        case 'get_auth_state':
            const sessionData = sessionStore.get(sessionId);
            ws.send(JSON.stringify({
                event: 'auth_state',
                data: {
                    isAuthenticated: !!(sessionData && sessionData.isAuthorized),
                    accountInfo: sessionData?.accountInfo || null
                }
            }));
            break;

        case 'start_bot':
            startBot(sessionId, msg.config);
            break;

        case 'stop_bot':
            stopBot(sessionId);
            break;

        case 'get_market_data':
            sendMarketData(sessionId, ws);
            break;

        case 'logout':
            handleLogout(sessionId, ws);
            break;
    }
}

function sendMarketData(sessionId, ws) {
    const wsMgr = wsConnections.get(sessionId);
    if (wsMgr) {
        const marketData = {};
        for (const [symbol, ticks] of wsMgr.tickBuffers.entries()) {
            if (ticks.length > 0) {
                marketData[symbol] = {
                    latestPrice: ticks[ticks.length - 1].price,
                    latestDigit: ticks[ticks.length - 1].digit,
                    tickCount: ticks.length,
                    digitDistribution: calculateDigitDistribution(ticks)
                };
            }
        }
        ws.send(JSON.stringify({ event: 'market_data', data: marketData }));
    }
}

function calculateDigitDistribution(ticks) {
    const counts = Array(10).fill(0);
    ticks.forEach(t => counts[t.digit]++);
    const total = ticks.length;
    return counts.map(c => ({
        count: c,
        percentage: total > 0 ? ((c / total) * 100).toFixed(2) : 0
    }));
}

// ============================================================
// BOT ENGINE
// ============================================================

const MARKETS = [
    'R_10', 'R_25', 'R_50', 'R_75', 'R_100',
    '1HZ10V', '1HZ25V', '1HZ50V', '1HZ75V', '1HZ100V',
    'JD10', 'JD25', 'JD50', 'JD75', 'JD100'
];

const STRATEGIES = {
    OVER_2: 'OVER_2',
    OVER_3: 'OVER_3',
    UNDER_6: 'UNDER_6',
    UNDER_7: 'UNDER_7',
    EVEN_ODD: 'EVEN_ODD',
    REVERSE_PSYCHOLOGY: 'REVERSE_PSYCHOLOGY',
    OVER_3_V2: 'OVER_3_V2',
    UNDER_7_V2: 'UNDER_7_V2'
};

class TradingBot {
    constructor(sessionId, config = {}) {
        this.sessionId = sessionId;
        this.config = {
            mode: config.mode || 'AI', // 'AI' or 'SINGLE'
            selectedStrategy: config.selectedStrategy || null,
            sampleWindow: config.sampleWindow || 1000,
            tickDuration: config.tickDuration || null, // null = auto
            baseStake: config.baseStake || 0.35,
            martingaleMultiplier: config.martingaleMultiplier || 2,
            consecutiveLossThreshold: config.consecutiveLossThreshold || 2,
            targetProfit: config.targetProfit || null,
            targetStopLoss: config.targetStopLoss || null,
            ...config
        };

        this.state = {
            isRunning: false,
            isPaused: false,
            activeTrade: null,
            currentStake: this.config.baseStake,
            consecutiveLosses: 0,
            totalProfit: 0,
            totalLoss: 0,
            netProfit: 0,
            tradeHistory: [],
            recoveryMode: false,
            lastStrategy: null,
            lastMarket: null,
            scanInterval: null
        };

        this.wsMgr = null;
    }

    async start() {
        if (this.state.isRunning) return;

        this.wsMgr = wsConnections.get(this.sessionId);
        if (!this.wsMgr || !this.wsMgr.isAuthorized) {
            throw new Error('Not connected or not authorized');
        }

        this.state.isRunning = true;
        this.state.currentStake = this.config.baseStake;

        // Subscribe to all markets
        MARKETS.forEach(symbol => this.wsMgr.subscribeTicks(symbol));

        // Start scanning loop
        this.state.scanInterval = setInterval(() => this.scan(), 1000);

        this.emit('bot_started', { config: this.config });
        console.log(`[Bot ${this.sessionId}] Started in ${this.config.mode} mode`);
    }

    stop() {
        this.state.isRunning = false;
        if (this.state.scanInterval) {
            clearInterval(this.state.scanInterval);
            this.state.scanInterval = null;
        }

        // Unsubscribe from markets
        if (this.wsMgr) {
            MARKETS.forEach(symbol => this.wsMgr.unsubscribeTicks(symbol));
        }

        this.emit('bot_stopped', { finalStats: this.getStats() });
        console.log(`[Bot ${this.sessionId}] Stopped`);
    }

    scan() {
        if (!this.state.isRunning || this.state.isPaused) return;
        if (this.state.activeTrade) return; // Wait for active trade to complete

        const candidates = [];

        for (const symbol of MARKETS) {
            const ticks = this.wsMgr.tickBuffers.get(symbol);
            if (!ticks || ticks.length < 50) continue; // Need minimum data

            const window = ticks.slice(-this.config.sampleWindow);
            const distribution = this.calculateDistribution(window);
            const bars = this.calculateBars(distribution);

            // Check each strategy
            const strategiesToCheck = this.config.mode === 'SINGLE' 
                ? [this.config.selectedStrategy] 
                : Object.values(STRATEGIES);

            for (const strategy of strategiesToCheck) {
                const result = this.evaluateStrategy(strategy, window, distribution, bars, symbol);
                if (result.valid) {
                    candidates.push(result);
                }
            }
        }

        if (candidates.length === 0) {
            this.emit('no_signal', { message: 'No Signal — Scanning...' });
            return;
        }

        // Sort by confidence score
        candidates.sort((a, b) => b.confidence - a.confidence);
        const best = candidates[0];

        // Check recovery mode
        if (this.state.recoveryMode && this.config.mode === 'AI') {
            // Force Even-Odd strategy
            const evenOddCandidates = candidates.filter(c => 
                c.strategy === STRATEGIES.EVEN_ODD
            );
            if (evenOddCandidates.length > 0) {
                this.executeTrade(evenOddCandidates[0]);
            }
        } else {
            this.executeTrade(best);
        }
    }

    calculateDistribution(ticks) {
        const counts = Array(10).fill(0);
        ticks.forEach(t => counts[t.digit]++);
        const total = ticks.length;
        return counts.map((count, digit) => ({
            digit,
            count,
            percentage: total > 0 ? (count / total) * 100 : 0
        }));
    }

    calculateBars(distribution) {
        const sorted = [...distribution].sort((a, b) => b.count - a.count);
        return {
            green: sorted[0],      // Most appearing
            blue: sorted[1],       // 2nd most
            yellow: sorted[8],     // 2nd least
            red: sorted[9]         // Least appearing
        };
    }

    evaluateStrategy(strategy, ticks, distribution, bars, symbol) {
        const lastTicks = ticks.slice(-10);
        const lastDigit = ticks[ticks.length - 1].digit;

        let result = { valid: false, strategy, symbol, confidence: 0, bars, distribution, lastDigit };

        switch (strategy) {
            case STRATEGIES.OVER_2:
                result = this.evaluateOver2(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.OVER_3:
                result = this.evaluateOver3(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.UNDER_6:
                result = this.evaluateUnder6(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.UNDER_7:
                result = this.evaluateUnder7(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.EVEN_ODD:
                result = this.evaluateEvenOdd(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.REVERSE_PSYCHOLOGY:
                result = this.evaluateReversePsychology(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.OVER_3_V2:
                result = this.evaluateOver3V2(ticks, distribution, bars, symbol);
                break;
            case STRATEGIES.UNDER_7_V2:
                result = this.evaluateUnder7V2(ticks, distribution, bars, symbol);
                break;
        }

        return result;
    }

    // ========== OVER 2 STRATEGY ==========
    evaluateOver2(ticks, distribution, bars, symbol) {
        const losingDigits = [0, 1, 2];
        const winningDigits = [3, 4, 5, 6, 7, 8, 9];

        // Check losing digits < 10.5%
        const losingBelowThreshold = losingDigits.every(d => 
            distribution[d].percentage < 10.5
        );
        if (!losingBelowThreshold) return { valid: false };

        // Check green bar in winning digits
        if (!winningDigits.includes(bars.green.digit)) return { valid: false };
        // Check red bar in losing digits
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        // Entry trigger: 3 consecutive losing digits then 1 winning digit
        const last4 = ticks.slice(-4).map(t => t.digit);
        const isTrigger = last4.length >= 4 &&
            losingDigits.includes(last4[0]) &&
            losingDigits.includes(last4[1]) &&
            losingDigits.includes(last4[2]) &&
            winningDigits.includes(last4[3]) &&
            last4[3] !== 0 && last4[3] !== 9; // Exclusion

        if (!isTrigger) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);
        return {
            valid: true,
            strategy: STRATEGIES.OVER_2,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITOVER',
            barrier: 2,
            duration: this.getDuration(symbol),
            entryDigit: last4[3]
        };
    }

    // ========== OVER 3 STRATEGY ==========
    evaluateOver3(ticks, distribution, bars, symbol) {
        const losingDigits = [0, 1, 2, 3];
        const winningDigits = [4, 5, 6, 7, 8, 9];

        const losingBelowThreshold = losingDigits.every(d => 
            distribution[d].percentage < 10.5
        );
        if (!losingBelowThreshold) return { valid: false };

        if (!winningDigits.includes(bars.green.digit)) return { valid: false };
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        const last4 = ticks.slice(-4).map(t => t.digit);
        const isTrigger = last4.length >= 4 &&
            losingDigits.includes(last4[0]) &&
            losingDigits.includes(last4[1]) &&
            losingDigits.includes(last4[2]) &&
            winningDigits.includes(last4[3]) &&
            last4[3] !== 0 && last4[3] !== 9;

        if (!isTrigger) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);
        return {
            valid: true,
            strategy: STRATEGIES.OVER_3,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITOVER',
            barrier: 3,
            duration: this.getDuration(symbol),
            entryDigit: last4[3]
        };
    }

    // ========== UNDER 6 STRATEGY ==========
    evaluateUnder6(ticks, distribution, bars, symbol) {
        const losingDigits = [6, 7, 8, 9];
        const winningDigits = [0, 1, 2, 3, 4, 5];

        const losingBelowThreshold = losingDigits.every(d => 
            distribution[d].percentage < 10.5
        );
        if (!losingBelowThreshold) return { valid: false };

        if (!winningDigits.includes(bars.green.digit)) return { valid: false };
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        const last4 = ticks.slice(-4).map(t => t.digit);
        const isTrigger = last4.length >= 4 &&
            losingDigits.includes(last4[0]) &&
            losingDigits.includes(last4[1]) &&
            losingDigits.includes(last4[2]) &&
            winningDigits.includes(last4[3]) &&
            last4[3] !== 0 && last4[3] !== 9;

        if (!isTrigger) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);
        return {
            valid: true,
            strategy: STRATEGIES.UNDER_6,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITUNDER',
            barrier: 6,
            duration: this.getDuration(symbol),
            entryDigit: last4[3]
        };
    }

    // ========== UNDER 7 STRATEGY ==========
    evaluateUnder7(ticks, distribution, bars, symbol) {
        const losingDigits = [7, 8, 9];
        const winningDigits = [0, 1, 2, 3, 4, 5, 6];

        const losingBelowThreshold = losingDigits.every(d => 
            distribution[d].percentage < 10.5
        );
        if (!losingBelowThreshold) return { valid: false };

        if (!winningDigits.includes(bars.green.digit)) return { valid: false };
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        const last4 = ticks.slice(-4).map(t => t.digit);
        const isTrigger = last4.length >= 4 &&
            losingDigits.includes(last4[0]) &&
            losingDigits.includes(last4[1]) &&
            losingDigits.includes(last4[2]) &&
            winningDigits.includes(last4[3]) &&
            last4[3] !== 0 && last4[3] !== 9;

        if (!isTrigger) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);
        return {
            valid: true,
            strategy: STRATEGIES.UNDER_7,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITUNDER',
            barrier: 7,
            duration: this.getDuration(symbol),
            entryDigit: last4[3]
        };
    }

    // ========== EVEN-ODD STRATEGY ==========
    evaluateEvenOdd(ticks, distribution, bars, symbol) {
        const oddDigits = [1, 3, 5, 7, 9];
        const evenDigits = [0, 2, 4, 6, 8];

        // All odd digits < 10.5% except exactly one
        const oddAboveThreshold = oddDigits.filter(d => distribution[d].percentage >= 10.5);
        if (oddAboveThreshold.length !== 1) return { valid: false };

        const entryDigit = oddAboveThreshold[0].digit;

        // Preceding tick must also be odd
        const last2 = ticks.slice(-2).map(t => t.digit);
        if (last2.length < 2 || !oddDigits.includes(last2[0])) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, evenDigits, oddDigits);
        return {
            valid: true,
            strategy: STRATEGIES.EVEN_ODD,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITEVEN',
            duration: this.getDuration(symbol),
            entryDigit
        };
    }

    // ========== REVERSE PSYCHOLOGY STRATEGY ==========
    evaluateReversePsychology(ticks, distribution, bars, symbol) {
        const evenDigits = [0, 2, 4, 6, 8];
        const oddDigits = [1, 3, 5, 7, 9];

        const evenCount = evenDigits.reduce((sum, d) => sum + distribution[d].count, 0);
        const oddCount = oddDigits.reduce((sum, d) => sum + distribution[d].count, 0);
        const total = evenCount + oddCount;

        const evenPct = (evenCount / total) * 100;
        const oddPct = (oddCount / total) * 100;

        let trigger = null;
        let contractType = null;

        // Trigger A: Red Bar Reversal
        if (evenPct > 55 && oddDigits.includes(bars.red.digit)) {
            // Even is dominant, red bar moved to odd side -> trade odd
            trigger = 'red_bar_reversal';
            contractType = 'DIGITODD';
        } else if (oddPct > 55 && evenDigits.includes(bars.red.digit)) {
            // Odd is dominant, red bar moved to even side -> trade even
            trigger = 'red_bar_reversal';
            contractType = 'DIGITEVEN';
        }

        // Trigger B: Percentage Recovery
        if (!trigger) {
            const weakSideDigits = evenPct > oddPct ? oddDigits : evenDigits;
            const recovering = weakSideDigits.find(d => distribution[d].percentage >= 10.5);
            if (recovering && (evenPct > 55 || oddPct > 55)) {
                trigger = 'percentage_recovery';
                contractType = evenPct > oddPct ? 'DIGITODD' : 'DIGITEVEN';
            }
        }

        if (!trigger) return { valid: false };

        const confidence = 50; // Base confidence for reverse psychology
        return {
            valid: true,
            strategy: STRATEGIES.REVERSE_PSYCHOLOGY,
            symbol,
            confidence,
            bars,
            distribution,
            contractType,
            duration: this.getDuration(symbol),
            trigger
        };
    }

    // ========== OVER 3 V2 STRATEGY (NEW) ==========
    evaluateOver3V2(ticks, distribution, bars, symbol) {
        const losingDigits = [0, 1, 2, 3];
        const winningDigits = [4, 5, 6, 7, 8, 9];

        // Check: losing digits < 10% OR if >= 10% must be trending down
        // For simplicity, we check current value < 10% or at 10% with previous lower
        const previousDistribution = this.getPreviousDistribution(symbol);

        const losingValid = losingDigits.every(d => {
            const current = distribution[d].percentage;
            if (current < 10) return true;
            if (current >= 10 && current <= 10.5) {
                // Check if trending down from previous
                if (previousDistribution) {
                    const prev = previousDistribution.find(x => x.digit === d);
                    return prev && current < prev.percentage;
                }
                return false;
            }
            return false;
        });

        if (!losingValid) return { valid: false };

        // Green bar must be among winning digits (4-9)
        if (!winningDigits.includes(bars.green.digit)) return { valid: false };

        // Red bar must be among losing digits (0-3)
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        // Entry trigger: single tap - the instant a tick lands on 0, 1, 2, or 3
        const lastDigit = ticks[ticks.length - 1].digit;
        if (!losingDigits.includes(lastDigit)) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);

        return {
            valid: true,
            strategy: STRATEGIES.OVER_3_V2,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITOVER',
            barrier: 3,
            duration: this.getDuration(symbol),
            entryDigit: lastDigit,
            isV2: true
        };
    }

    // ========== UNDER 7 V2 STRATEGY (NEW) ==========
    evaluateUnder7V2(ticks, distribution, bars, symbol) {
        const losingDigits = [7, 8, 9];
        const winningDigits = [0, 1, 2, 3, 4, 5, 6];

        // Check: losing digits < 10% OR if >= 10% must be trending down
        const previousDistribution = this.getPreviousDistribution(symbol);

        const losingValid = losingDigits.every(d => {
            const current = distribution[d].percentage;
            if (current < 10) return true;
            if (current >= 10 && current <= 10.5) {
                if (previousDistribution) {
                    const prev = previousDistribution.find(x => x.digit === d);
                    return prev && current < prev.percentage;
                }
                return false;
            }
            return false;
        });

        if (!losingValid) return { valid: false };

        // Green bar must be among winning digits (0-6)
        if (!winningDigits.includes(bars.green.digit)) return { valid: false };

        // Red bar must be among losing digits (7-9)
        if (!losingDigits.includes(bars.red.digit)) return { valid: false };

        // Entry trigger: single tap - the instant a tick lands on 7, 8, or 9
        const lastDigit = ticks[ticks.length - 1].digit;
        if (!losingDigits.includes(lastDigit)) return { valid: false };

        const confidence = this.calculateConfidence(distribution, bars, winningDigits, losingDigits);

        return {
            valid: true,
            strategy: STRATEGIES.UNDER_7_V2,
            symbol,
            confidence,
            bars,
            distribution,
            contractType: 'DIGITUNDER',
            barrier: 7,
            duration: this.getDuration(symbol),
            entryDigit: lastDigit,
            isV2: true
        };
    }

    // Store previous distribution for trend checking
    getPreviousDistribution(symbol) {
        // In a real implementation, you'd store historical distributions
        // For now, return null (simplified)
        return null;
    }

    calculateConfidence(distribution, bars, winningDigits, losingDigits) {
        // 50% - Percentage Strength (how far losing digits below 10.5%)
        let percentageStrength = 0;
        const losingPcts = losingDigits.map(d => distribution[d].percentage);
        const avgLosing = losingPcts.reduce((a, b) => a + b, 0) / losingPcts.length;
        percentageStrength = Math.max(0, Math.min(50, (10.5 - avgLosing) * 5));

        // 30% - Bar Alignment (green & blue on winning side)
        let barAlignment = 0;
        if (winningDigits.includes(bars.green.digit)) barAlignment += 15;
        if (winningDigits.includes(bars.blue.digit)) barAlignment += 15;

        // 20% - Winning Side Strength Count (2+ winning digits at 11%+)
        let winningStrength = 0;
        const strongWinners = winningDigits.filter(d => distribution[d].percentage >= 11);
        if (strongWinners.length >= 2) winningStrength = 20;
        else if (strongWinners.length === 1) winningStrength = 10;

        return Math.round(percentageStrength + barAlignment + winningStrength);
    }

    getDuration(symbol) {
        if (this.config.tickDuration) return this.config.tickDuration;

        // Auto-detect based on symbol
        if (symbol.startsWith('JD') || symbol.startsWith('1HZ')) {
            return 2; // Jump and Seconds = 2 ticks (new accounts)
        }
        return 1; // Plain volatility = 1 tick
    }

    async executeTrade(candidate) {
        if (this.state.activeTrade) return;

        try {
            // Get proposal
            const proposal = await this.wsMgr.getProposal({
                proposal: 1,
                amount: this.state.currentStake,
                basis: 'stake',
                contract_type: candidate.contractType,
                currency: this.wsMgr.accountInfo.currency || 'USD',
                duration: candidate.duration,
                duration_unit: 't',
                symbol: candidate.symbol,
                barrier: candidate.barrier
            });

            if (!proposal.proposal) {
                this.emit('trade_error', { error: 'No proposal received', candidate });
                return;
            }

            // Buy contract
            const buyResult = await this.wsMgr.buyContract(
                proposal.proposal.id,
                proposal.proposal.ask_price
            );

            this.state.activeTrade = {
                contractId: buyResult.buy.contract_id,
                strategy: candidate.strategy,
                symbol: candidate.symbol,
                stake: this.state.currentStake,
                contractType: candidate.contractType,
                barrier: candidate.barrier,
                startTime: Date.now(),
                confidence: candidate.confidence
            };

            this.state.lastStrategy = candidate.strategy;
            this.state.lastMarket = candidate.symbol;

            this.emit('trade_executed', this.state.activeTrade);

            // Monitor trade completion
            this.monitorTrade(buyResult.buy.contract_id);

        } catch (err) {
            console.error(`[Bot ${this.sessionId}] Trade error:`, err.message);
            this.emit('trade_error', { error: err.message, candidate });
        }
    }

    async monitorTrade(contractId) {
        // Poll for contract result
        const checkInterval = setInterval(async () => {
            try {
                const response = await this.wsMgr.sendWithResponse({
                    proposal_open_contract: 1,
                    contract_id: contractId
                });

                if (response.proposal_open_contract && 
                    response.proposal_open_contract.is_sold) {

                    clearInterval(checkInterval);
                    const contract = response.proposal_open_contract;
                    const profit = parseFloat(contract.profit);
                    const isWin = profit > 0;

                    this.handleTradeResult(isWin, profit, contract);
                }
            } catch (err) {
                console.error(`[Bot ${this.sessionId}] Monitor error:`, err.message);
            }
        }, 1000);
    }

    handleTradeResult(isWin, profit, contract) {
        const trade = {
            ...this.state.activeTrade,
            result: isWin ? 'WIN' : 'LOSS',
            profit: profit,
            exitPrice: contract.exit_tick,
            exitDigit: contract.exit_tick ? parseInt(contract.exit_tick.toString().slice(-1)) : null,
            endTime: Date.now()
        };

        this.state.tradeHistory.push(trade);
        this.state.activeTrade = null;

        if (isWin) {
            this.state.totalProfit += profit;
            this.state.netProfit += profit;
            this.state.consecutiveLosses = 0;
            this.state.currentStake = this.config.baseStake;

            if (this.state.recoveryMode) {
                this.state.recoveryMode = false;
            }

            this.emit('trade_win', trade);
        } else {
            this.state.totalLoss += Math.abs(profit);
            this.state.netProfit -= Math.abs(profit);
            this.state.consecutiveLosses++;
            this.state.currentStake *= this.config.martingaleMultiplier;

            // Check recovery mode trigger (AI mode only)
            if (this.config.mode === 'AI' && 
                this.state.consecutiveLosses >= this.config.consecutiveLossThreshold &&
                [STRATEGIES.OVER_2, STRATEGIES.OVER_3, STRATEGIES.UNDER_6, 
                 STRATEGIES.UNDER_7, STRATEGIES.REVERSE_PSYCHOLOGY].includes(this.state.lastStrategy)) {
                this.state.recoveryMode = true;
            }

            this.emit('trade_loss', trade);
        }

        // Check exit conditions
        this.checkExitConditions();
    }

    checkExitConditions() {
        if (this.config.targetProfit && this.state.netProfit >= this.config.targetProfit) {
            this.emit('target_profit_reached', { netProfit: this.state.netProfit });
            this.stop();
            return;
        }

        if (this.config.targetStopLoss && this.state.netProfit <= -this.config.targetStopLoss) {
            this.emit('stop_loss_reached', { netProfit: this.state.netProfit });
            this.stop();
            return;
        }
    }

    getStats() {
        return {
            isRunning: this.state.isRunning,
            isPaused: this.state.isPaused,
            activeTrade: this.state.activeTrade,
            currentStake: this.state.currentStake,
            consecutiveLosses: this.state.consecutiveLosses,
            totalProfit: this.state.totalProfit,
            totalLoss: this.state.totalLoss,
            netProfit: this.state.netProfit,
            tradeCount: this.state.tradeHistory.length,
            winCount: this.state.tradeHistory.filter(t => t.result === 'WIN').length,
            lossCount: this.state.tradeHistory.filter(t => t.result === 'LOSS').length,
            recoveryMode: this.state.recoveryMode,
            lastStrategy: this.state.lastStrategy,
            lastMarket: this.state.lastMarket
        };
    }

    emit(event, data) {
        const clients = dashboardClients.get(this.sessionId);
        if (clients) {
            clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ event, data }));
                }
            });
        }
    }
}

// ============================================================
// BOT MANAGEMENT
// ============================================================

function startBot(sessionId, config) {
    if (botStates.has(sessionId)) {
        const existing = botStates.get(sessionId);
        if (existing.state.isRunning) {
            return; // Already running
        }
    }

    const bot = new TradingBot(sessionId, config);
    botStates.set(sessionId, bot);
    bot.start().catch(err => {
        console.error(`[Bot ${sessionId}] Start failed:`, err.message);
        const clients = dashboardClients.get(sessionId);
        if (clients) {
            clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({
                        event: 'bot_error',
                        data: { error: err.message }
                    }));
                }
            });
        }
    });
}

function stopBot(sessionId) {
    const bot = botStates.get(sessionId);
    if (bot) {
        bot.stop();
    }
}

// ============================================================
// AUTHENTICATION ROUTES
// ============================================================

/**
 * STEP 1: Initiate OAuth 2.0 + PKCE Flow
 * Generates code_verifier, code_challenge, state
 * Redirects user to Deriv authorization endpoint
 */
app.get('/auth/deriv', (req, res) => {
    try {
        const codeVerifier = generateCodeVerifier();
        const codeChallenge = generateCodeChallenge(codeVerifier);
        const state = generateState();

        // Store PKCE parameters
        pkceStore.set(state, {
            code_verifier: codeVerifier,
            code_challenge: codeChallenge,
            createdAt: Date.now()
        });

        // Build authorization URL
        const params = new URLSearchParams({
            response_type: 'code',
            client_id: CONFIG.DERIV_APP_ID,
            redirect_uri: CONFIG.DERIV_REDIRECT_URI,
            scope: 'trade account_manage',
            state: state,
            code_challenge: codeChallenge,
            code_challenge_method: 'S256'
        });

        const authUrl = `${CONFIG.DERIV_AUTHORIZE_URL}?${params.toString()}`;

        console.log(`[Auth] Redirecting to Deriv OAuth for session ${req.sessionID}`);
        res.redirect(authUrl);

    } catch (err) {
        console.error('[Auth] Initiation error:', err.message);
        res.status(500).json({ error: 'Authentication initiation failed', details: err.message });
    }
});

/**
 * STEP 2: OAuth Callback Handler
 * Receives authorization code from Deriv
 * Exchanges code for access token
 * Establishes WebSocket connection and authorizes
 */
app.get('/auth/deriv/callback', async (req, res) => {
    const { code, state, error, error_description } = req.query;

    // Handle OAuth errors from Deriv
    if (error) {
        console.error(`[Auth] OAuth error: ${error} - ${error_description}`);
        return res.redirect('/?auth_error=' + encodeURIComponent(error_description || error));
    }

    // Validate state parameter (CSRF protection)
    if (!state || !pkceStore.has(state)) {
        console.error('[Auth] Invalid or missing state parameter');
        return res.redirect('/?auth_error=invalid_state');
    }

    const pkceData = pkceStore.get(state);
    pkceStore.delete(state); // One-time use

    // Validate authorization code
    if (!code) {
        console.error('[Auth] No authorization code received');
        return res.redirect('/?auth_error=no_code');
    }

    try {
        // STEP 3: Exchange authorization code for access token
        const tokenResponse = await fetch(CONFIG.DERIV_TOKEN_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: new URLSearchParams({
                grant_type: 'authorization_code',
                client_id: CONFIG.DERIV_APP_ID,
                code: code,
                redirect_uri: CONFIG.DERIV_REDIRECT_URI,
                code_verifier: pkceData.code_verifier
            }).toString()
        });

        const tokenData = await tokenResponse.json();

        if (!tokenResponse.ok || tokenData.error) {
            console.error('[Auth] Token exchange failed:', tokenData.error_description || tokenData.error);
            return res.redirect('/?auth_error=' + encodeURIComponent(tokenData.error_description || 'token_exchange_failed'));
        }

        const { access_token, refresh_token, expires_in, scope } = tokenData;

        console.log(`[Auth] Token received, expires in ${expires_in}s`);

        // STEP 4: Store authenticated session
        const sessionData = {
            accessToken: access_token,
            refreshToken: refresh_token,
            tokenExpiresAt: Date.now() + (expires_in * 1000),
            scope: scope,
            isAuthorized: false,
            accountInfo: null,
            loginTime: Date.now()
        };

        sessionStore.set(req.sessionID, sessionData);

        // STEP 5: Establish WebSocket connection and authorize
        const wsMgr = new DerivWebSocketManager(req.sessionID);
        wsConnections.set(req.sessionID, wsMgr);

        await wsMgr.connect();
        const accountInfo = await wsMgr.authorize(access_token);

        console.log(`[Auth] User ${accountInfo.loginid} fully authenticated`);

        // Redirect to dashboard with success
        res.redirect('/?auth=success');

    } catch (err) {
        console.error('[Auth] Callback processing error:', err.message);
        res.redirect('/?auth_error=' + encodeURIComponent(err.message));
    }
});

/**
 * Check authentication status
 */
app.get('/api/auth/status', (req, res) => {
    const sessionData = sessionStore.get(req.sessionID);

    if (!sessionData) {
        return res.json({
            isAuthenticated: false,
            accountInfo: null
        });
    }

    // Check token expiry
    const isExpired = sessionData.tokenExpiresAt && Date.now() > sessionData.tokenExpiresAt;

    res.json({
        isAuthenticated: sessionData.isAuthorized && !isExpired,
        isExpired: isExpired,
        accountInfo: sessionData.accountInfo,
        loginTime: sessionData.loginTime
    });
});

/**
 * Logout - clear session and disconnect WebSocket
 */
app.post('/api/auth/logout', (req, res) => {
    const sessionId = req.sessionID;

    // Disconnect WebSocket
    const wsMgr = wsConnections.get(sessionId);
    if (wsMgr) {
        wsMgr.disconnect();
    }

    // Clear session data
    sessionStore.delete(sessionId);
    botStates.delete(sessionId);

    // Destroy express session
    req.session.destroy();

    res.json({ success: true, message: 'Logged out successfully' });
});

function handleLogout(sessionId, ws) {
    const wsMgr = wsConnections.get(sessionId);
    if (wsMgr) {
        wsMgr.disconnect();
    }
    sessionStore.delete(sessionId);
    botStates.delete(sessionId);
    ws.send(JSON.stringify({ event: 'logged_out', data: {} }));
}

// ============================================================
// BOT API ROUTES
// ============================================================

app.post('/api/bot/start', (req, res) => {
    const sessionData = sessionStore.get(req.sessionID);
    if (!sessionData || !sessionData.isAuthorized) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const config = req.body;
    startBot(req.sessionID, config);
    res.json({ success: true, message: 'Bot starting...' });
});

app.post('/api/bot/stop', (req, res) => {
    stopBot(req.sessionID);
    res.json({ success: true, message: 'Bot stopping...' });
});

app.get('/api/bot/stats', (req, res) => {
    const bot = botStates.get(req.sessionID);
    if (!bot) {
        return res.json({ isRunning: false });
    }
    res.json(bot.getStats());
});

app.get('/api/bot/history', (req, res) => {
    const bot = botStates.get(req.sessionID);
    if (!bot) {
        return res.json({ trades: [] });
    }
    res.json({ trades: bot.state.tradeHistory });
});

// ============================================================
// HEALTH CHECK
// ============================================================
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        timestamp: new Date().toISOString(),
        activeSessions: sessionStore.size,
        activeBots: Array.from(botStates.values()).filter(b => b.state.isRunning).length,
        activeWebSockets: wsConnections.size
    });
});

// ============================================================
// SERVE DASHBOARD
// ============================================================
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================================
// WEBSOCKET UPGRADE HANDLING
// ============================================================
const server = app.listen(PORT, () => {
    console.log(`============================================`);
    console.log(`Deriv Digit Trading Bot v2.0.0`);
    console.log(`Server running on port ${PORT}`);
    console.log(`OAuth Redirect: ${CONFIG.DERIV_REDIRECT_URI}`);
    console.log(`============================================`);
});

server.on('upgrade', (request, socket, head) => {
    // Parse session from cookie
    const cookie = request.headers.cookie;
    let sessionID = null;

    if (cookie) {
        const match = cookie.match(/derivBotSession=([^;]+)/);
        if (match) {
            // Decode the signed cookie (simplified - in production use proper session parsing)
            sessionID = decodeURIComponent(match[1]).split('.')[0];
        }
    }

    request.sessionID = sessionID || 'anonymous-' + Date.now();

    dashboardWSS.handleUpgrade(request, socket, head, (ws) => {
        dashboardWSS.emit('connection', ws, request);
    });
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received, shutting down gracefully');
    for (const [sessionId, wsMgr] of wsConnections.entries()) {
        wsMgr.disconnect();
    }
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});