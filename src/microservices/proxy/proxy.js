require('dotenv').config();
const express = require('express');
const {createProxyMiddleware} = require('http-proxy-middleware');
const morgan = require('morgan');
const app = express();
const PORT = process.env.PORT || 8000;

// Конфигурация сервисов
const SERVICE_CONFIG = {
    gradualMigration: process.env.GRADUAL_MIGRATION || false,
    movies: {
        url: process.env.MOVIES_SERVICE_URL || 'http://movies-service:8081',
        weight: parseInt(process.env.MOVIES_MIGRATION_PERCENT) || 0
    },
    monolith: process.env.MONOLITH_URL || 'http://monolith:8080'
};

// Middleware
app.use(express.json());
app.use(morgan('combined'));

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({status: 'OK'});
});

// Функция выбора сервиса
function selectServiceVersion(serviceName) {
    const serviceConfig = SERVICE_CONFIG[serviceName];
    if (!serviceConfig) return SERVICE_CONFIG.monolith;

    if (SERVICE_CONFIG.gradualMigration) {
        const {url, weight} = serviceConfig;
        const random = Math.random() * 100;

        if (random <= weight) {
            return url;
        }
    }

    return SERVICE_CONFIG.monolith;
}

// Прокси для /api/users
app.use('/api', (req, res, next) => {
    console.log(`Proxying ${req.method} ${req.originalUrl}`, req.body);

    // Динамическое определение целевого URL
    const targetUrl = req.path.startsWith('/movies')
        ? selectServiceVersion('movies')
        : SERVICE_CONFIG.monolith;

    createProxyMiddleware({
        target: targetUrl,
        changeOrigin: true,
        onProxyReq: (proxyReq, req) => {
            if (req.body) {
                const bodyData = JSON.stringify(req.body);
                proxyReq.setHeader('Content-Type', 'application/json');
                proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
                proxyReq.write(bodyData);
            }
        },
        onProxyRes: (proxyRes) => {
            console.log('Proxy response status:', proxyRes.statusCode);
        },
        onError: (err, req, res) => {
            console.error('Proxy error:', err);
            if (!res.headersSent) {
                res.status(502).json({error: 'Bad Gateway'});
            }
        }
    })(req, res, next);
});

// Обработка ошибок
app.use((err, req, res, next) => {
    console.error(err.stack);
    if (res.status) {
        res.status(500).json({error: 'Internal Server Error'});
    } else {
        res.writeHead(500, {'Content-Type': 'application/json'});
        res.end(JSON.stringify({error: 'Internal Server Error'}));
    }
});

app.listen(PORT, () => {
    console.log(`API Gateway running on port ${PORT}`);
    console.log('Services configuration:');
    console.log(JSON.stringify(SERVICE_CONFIG, null, 2));
});