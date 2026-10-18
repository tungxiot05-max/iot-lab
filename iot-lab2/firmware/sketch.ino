/*
 * Bài thực hành 2 – Firmware nút cảm biến ESP32 (nâng cấp từ Bài 1)
 * Cảm biến: DHT22 (nhiệt độ, độ ẩm), PIR (chuyển động), LDR (ánh sáng)
 * Gửi JSON qua MQTT lên broker, kèm timestamp NTP để đo độ trễ end-to-end.
 */
#define ARDUINOJSON_USE_LONG_LONG 1
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include "DHT.h"

// ================== CẤU HÌNH (sửa tại đây) ==================
#define WIFI_SSID   "Wokwi-GUEST"
#define WIFI_PASS   ""

// Wokwi không truy cập được localhost -> dùng broker public.
#define MQTT_HOST   "broker.hivemq.com"
#define MQTT_PORT   1883

#define SITE        "ptit"              // đổi thành tên nhóm/lớp
#define LOCATION    "lab"               // tag vị trí
#define DEVICE_ID   "esp32-tung-01"     // nên chứa MSSV để không trùng

#define PUBLISH_INTERVAL_MS 2000        // DHT22 cần tối thiểu 2 giây

// 1 = thỉnh thoảng tạo spike / mất gói giả lập để có dữ liệu thử tiền xử lý.
// Nhớ ghi rõ trong báo cáo nếu bật.
#define SIMULATE_FAULTS 0
// =============================================================

#define DHT_PIN   15
#define PIR_PIN   13
#define LDR_PIN   34                    // chân AO của module quang trở
#define LED_PIN   2
#define DHT_TYPE  DHT22

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient net;
PubSubClient mqtt(net);

char topicTelemetry[96];
char topicStatus[96];
uint32_t seq = 0;
unsigned long lastPublish = 0;

// ---------- Tiện ích ----------
uint64_t epochMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

float round1(float v) { return roundf(v * 10.0f) / 10.0f; }

// Đổi giá trị ADC của module quang trở Wokwi sang lux (theo tài liệu Wokwi).
float adcToLux(int adc) {
  const float GAMMA = 0.7f, RL10 = 50.0f, VCC = 3.3f;
  float voltage = adc / 4095.0f * VCC;
  if (voltage >= VCC - 0.01f) voltage = VCC - 0.01f;   // tránh chia cho 0
  if (voltage <= 0.001f) return 0.0f;
  float resistance = 2000.0f * voltage / (1.0f - voltage / VCC);
  return powf(RL10 * 1e3f * powf(10.0f, GAMMA) / resistance, 1.0f / GAMMA);
}

// ---------- Kết nối ----------
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.print("[WiFi] Connecting");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) {
    delay(250);
    Serial.print(".");
  }
  Serial.printf("\n[WiFi] Connected, IP=%s\n", WiFi.localIP().toString().c_str());
}

void syncTime() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.print("[NTP] Syncing");
  unsigned long start = millis();
  while (time(nullptr) < 1700000000 && millis() - start < 15000) {
    delay(250);
    Serial.print(".");
  }
  Serial.printf("\n[NTP] %s, epoch_ms=%llu\n",
                time(nullptr) > 1700000000 ? "OK" : "FAILED",
                (unsigned long long)epochMs());
}

void connectMQTT() {
  while (!mqtt.connected()) {
    char clientId[48];
    snprintf(clientId, sizeof(clientId), "%s-%04X", DEVICE_ID, (unsigned)random(0xFFFF));
    Serial.printf("[MQTT] Connecting to %s:%d as %s ... ", MQTT_HOST, MQTT_PORT, clientId);
    // Last Will: broker tự đăng "offline" nếu thiết bị mất kết nối đột ngột
    if (mqtt.connect(clientId, nullptr, nullptr, topicStatus, 1, true, "offline")) {
      Serial.println("OK");
      mqtt.publish(topicStatus, "online", true);
    } else {
      Serial.printf("failed rc=%d, retry in 2s\n", mqtt.state());
      delay(2000);
      if (WiFi.status() != WL_CONNECTED) connectWiFi();
    }
  }
}

// ---------- Chương trình chính ----------
void setup() {
  Serial.begin(115200);
  pinMode(PIR_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  analogReadResolution(12);
  dht.begin();
  randomSeed(esp_random());

  snprintf(topicTelemetry, sizeof(topicTelemetry), "iot/%s/%s/%s/telemetry", SITE, LOCATION, DEVICE_ID);
  snprintf(topicStatus, sizeof(topicStatus), "iot/%s/%s/%s/status", SITE, LOCATION, DEVICE_ID);
  Serial.printf("[CFG] Telemetry topic: %s\n", topicTelemetry);

  connectWiFi();
  syncTime();
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  mqtt.setKeepAlive(30);
  connectMQTT();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!mqtt.connected()) connectMQTT();
  mqtt.loop();

  unsigned long now = millis();
  if (now - lastPublish < PUBLISH_INTERVAL_MS) return;
  lastPublish = now;

  float temperature = dht.readTemperature();
  float humidity = dht.readHumidity();
  int motion = digitalRead(PIR_PIN);
  int lightRaw = analogRead(LDR_PIN);

  // Kiểm tra hợp lệ tại thiết bị: không gửi gói DHT lỗi
  if (isnan(temperature) || isnan(humidity)) {
    Serial.println("[WARN] DHT read failed -> skip");
    return;
  }

  seq++;

#if SIMULATE_FAULTS
  if (random(100) < 3) {                 // ~3%: mất gói giả lập (seq vẫn tăng)
    Serial.printf("[SIM] Drop packet seq=%u\n", seq);
    return;
  }
  if (random(100) < 3) {                 // ~3%: spike giả lập
    temperature += (random(2) ? 35.0f : -35.0f);
    Serial.println("[SIM] Inject temperature spike");
  }
#endif

  JsonDocument doc;
  doc["device_id"]   = DEVICE_ID;
  doc["location"]    = LOCATION;
  doc["seq"]         = seq;
  doc["ts"]          = (long long)epochMs();   // ms since epoch (UTC)
  doc["temperature"] = round1(temperature);
  doc["humidity"]    = round1(humidity);
  doc["light"]       = round1(adcToLux(lightRaw));
  doc["light_raw"]   = lightRaw;
  doc["motion"]      = motion;
  doc["rssi"]        = WiFi.RSSI();
  doc["uptime_s"]    = (uint32_t)(millis() / 1000);

  char payload[384];
  size_t n = serializeJson(doc, payload, sizeof(payload));

  if (mqtt.publish(topicTelemetry, (const uint8_t*)payload, n, false)) {
    Serial.printf("[PUB] %s\n", payload);
    digitalWrite(LED_PIN, HIGH);
    delay(30);
    digitalWrite(LED_PIN, LOW);
  } else {
    Serial.printf("[ERR] Publish failed seq=%u\n", seq);
  }
}
