require('dotenv').config();
const express = require('express');
const { Kafka } = require('kafkajs');
const morgan = require('morgan');

const app = express();
const PORT = process.env.PORT || 8082;

// Типы событий
class Event {
    constructor(type, payload) {
        this.id = `${type}-${Date.now()}`;
        this.type = type;
        this.timestamp = new Date();
        this.payload = payload;
    }
}

class MovieEvent {
    constructor(movieId, title, action, userId, rating, genres, description) {
        this.movie_id = movieId;
        this.title = title;
        this.action = action;
        this.user_id = userId;
        this.rating = rating;
        this.genres = genres;
        this.description = description;
    }
}

class UserEvent {
    constructor(userId, username, email, action) {
        this.user_id = userId;
        this.username = username;
        this.email = email;
        this.action = action;
        this.timestamp = new Date();
    }
}

class PaymentEvent {
    constructor(paymentId, userId, amount, status, methodType) {
        this.payment_id = paymentId;
        this.user_id = userId;
        this.amount = amount;
        this.status = status;
        this.timestamp = new Date();
        this.method_type = methodType;
    }
}

// Инициализация Kafka
const kafka = new Kafka({
    clientId: 'events-service',
    brokers: [process.env.KAFKA_BROKERS || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'events-service-group' });

// Middleware
app.use(express.json());
app.use(morgan('combined'));

// Подключение к Kafka
async function connectKafka() {
    await producer.connect();
    await consumer.connect();

    // Подписка на топики
    await consumer.subscribe({ topic: 'movie-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'user-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });

    console.log('Connected to Kafka');

    // Запуск потребителя
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message from topic ${topic}:`, message.value.toString());
            await processMessage(topic, message.value.toString());
        },
    });
}

// Обработка сообщений
async function processMessage(topic, message) {
    try {
        const event = JSON.parse(message);

        switch (topic) {
            case 'movie-events':
                console.log('Processing movie event:', event);
                // Логика обработки событий о фильмах
                break;
            case 'user-events':
                console.log('Processing user event:', event);
                // Логика обработки событий пользователей
                break;
            case 'payment-events':
                console.log('Processing payment event:', event);
                // Логика обработки платежных событий
                break;
        }
    } catch (err) {
        console.error(`Error processing message from ${topic}:`, err);
    }
}

// Маршруты API
app.get('/api/events/health', (req, res) => {
    res.json({ status: true });
});

app.post('/api/events/movie', async (req, res) => {
    try {
        const movieEvent = new MovieEvent(
            req.body.movie_id,
            req.body.title,
            req.body.action,
            req.body.user_id,
            req.body.rating,
            req.body.genres,
            req.body.description
        );

        const event = new Event('movie', movieEvent);
        await sendToKafka('movie-events', event);

        res.status(201).json({
            status: 'success',
            event: event
        });
    } catch (err) {
        console.error('Error handling movie event:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/events/user', async (req, res) => {
    try {
        const userEvent = new UserEvent(
            req.body.user_id,
            req.body.username,
            req.body.email,
            req.body.action
        );

        const event = new Event('user', userEvent);
        await sendToKafka('user-events', event);

        res.status(201).json({
            status: 'success',
            event: event
        });
    } catch (err) {
        console.error('Error handling user event:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/events/payment', async (req, res) => {
    try {
        const paymentEvent = new PaymentEvent(
            req.body.payment_id,
            req.body.user_id,
            req.body.amount,
            req.body.status,
            req.body.method_type
        );

        const event = new Event('payment', paymentEvent);
        await sendToKafka('payment-events', event);

        res.status(201).json({
            status: 'success',
            event: event
        });
    } catch (err) {
        console.error('Error handling payment event:', err);
        res.status(500).json({ error: err.message });
    }
});

// Отправка сообщения в Kafka
async function sendToKafka(topic, event) {
    try {
        const result = await producer.send({
            topic,
            messages: [
                { value: JSON.stringify(event) }
            ]
        });

        console.log(`Event sent to ${topic}`, result);
        return result;
    } catch (err) {
        console.error(`Error sending to ${topic}:`, err);
        throw err;
    }
}

// Запуск сервера
async function startServer() {
    try {
        await connectKafka();
        app.listen(PORT, () => {
            console.log(`Events service running on port ${PORT}`);
        });
    } catch (err) {
        console.error('Failed to start server:', err);
        process.exit(1);
    }
}

// Обработка завершения работы
process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    await producer.disconnect();
    await consumer.disconnect();
    process.exit(0);
});

startServer();