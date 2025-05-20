# Challenges Faced During Implementation

## Part 1: Real-Time Market Data Publisher

### 1. MQTT Connection Issues
Initially, the application failed to connect to the MQTT broker (`emqx.trado.trade:8883`). The error was due to using the wrong protocol (`mqtt://` instead of `mqtts://`) and missing TLS options. I resolved this by:
- Switching to `mqtts://` protocol.
  ```typescript
  const connectUrl = `mqtts://${config.mqtt.host}:${config.mqtt.port}`;
  
  ```

