# Live Crypto Price Dashboard with Kafka, WebSocket, and MongoDB

## Overview

This project implements a real-time cryptocurrency price dashboard that streams live market data using Kafka as the message broker, MongoDB for data persistence, and WebSocket for real-time updates to the frontend. The dashboard provides interactive charts and key statistics for multiple cryptocurrencies, enabling users to monitor price movements and trends dynamically.

The system is designed to be scalable, efficient, and modular, supporting seamless integration of additional data sources and features.

## Features

- Real-time streaming of cryptocurrency prices via Kafka topics
- Persistent storage and querying of historical price data in MongoDB
- WebSocket connection for pushing live updates to the frontend React dashboard
- Interactive line charts powered by Chart.js with selectable time ranges (1 hour, 24 hours)
- Display of key metrics including current price, price changes, high/low, and volume
- Dynamic coin selection (Bitcoin, Ethereum, Ripple)
- Responsive and user-friendly interface with Tailwind CSS styling

## Technology Stack

- **Kafka**: Distributed event streaming platform for handling live price updates
- **MongoDB**: NoSQL database to store historical price data
- **Node.js + WebSocket**: Backend server facilitating WebSocket communication and data delivery
- **React.js + Chart.js**: Frontend UI with live updating charts
- **Tailwind CSS**: Styling for a clean and modern interface

## Architecture

1. Data producers fetch or receive live crypto prices and publish messages to Kafka topics.
2. A Node.js WebSocket server consumes Kafka messages, stores them in MongoDB, and broadcasts updates to connected clients.
3. React frontend connects via WebSocket, receives live data, and updates charts and stats accordingly.

## Getting Started

### Prerequisites

- Node.js (v16+)
- MongoDB (running locally or accessible remotely)
- Kafka cluster or local Kafka setup
- Yarn or npm for frontend dependencies

### Installation

1. Clone the repository:

```bash
git clone https://github.com/simobls/Crypto_dashboard_kafka.git
cd Crypto_dashboard_kafka
