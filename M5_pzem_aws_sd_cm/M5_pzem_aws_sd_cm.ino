// Include necessary libraries
#include "secrets.h" // File containing your secrets like Wi-Fi credentials and AWS endpoint
#include <WiFiClientSecure.h> // Library for secure Wi-Fi client
#include <MQTTClient.h> // Library for MQTT communication
#include <ArduinoJson.h> // Library for handling JSON data
#include "WiFi.h" // Library for Wi-Fi functionality
#include <Wire.h> // Library for I2C communication
#include <time.h> // Library for time functionality
#include <PZEM004Tv30.h> // Library for PZEM004Tv30 sensor
#include <M5Stack.h> // Library for M5Stack display

// Define default PZEM004Tv30 pins if not defined
#if !defined(PZEM_RX_PIN) && !defined(PZEM_TX_PIN)
#define PZEM_RX_PIN 16
#define PZEM_TX_PIN 17
#endif

// Define default PZEM004Tv30 serial if not defined
#if !defined(PZEM_SERIAL)
#define PZEM_SERIAL Serial2
#endif

#define BUFFER_SIZE 60 // Adjust buffer size as needed

// Declare variables
int bufferIndex = 0;
int fileIndex = 1;
String bufferData[BUFFER_SIZE];
float voltage = 0.0;
float current = 0.0;
float power = 0.0;
float energy = 0.0;
float frequency = 0.0;
float pf = 0.0;
char* buffer_data;
bool bufferHasData = false;

// Initialize PZEM004Tv30 sensor
#if defined(ESP32)
PZEM004Tv30 pzem(PZEM_SERIAL, PZEM_RX_PIN, PZEM_TX_PIN);
#elif defined(ESP8266)
PZEM004Tv30 pzem(PZEM_SERIAL);
#endif

// MQTT topics
#define AWS_IOT_PUBLISH_TOPIC   "M5/pub/pzem"
#define AWS_IOT_SUBSCRIBE_TOPIC "M5/sub"

//Set up MQTT
WiFiClientSecure net = WiFiClientSecure();
MQTTClient client = MQTTClient(256);

//Define timezone GMT+7
long timezone = +7; 
byte daysavetime = 0;

//Create time variable
struct tm tmstruct;
char buffer[30];

// Function to connect to AWS IoT
void connectAWS() {
  // Connect to Wi-Fi
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.println("Connecting to Wi-Fi");
  while (WiFi.status() != WL_CONNECTED){
    delay(500);
    Serial.print(".");
  }

  // Configure WiFiClientSecure with AWS IoT credentials
  net.setCACert(AWS_CERT_CA);
  net.setCertificate(AWS_CERT_CRT);
  net.setPrivateKey(AWS_CERT_PRIVATE);

  // Connect to AWS IoT MQTT broker
  client.begin(AWS_IOT_ENDPOINT, 8883, net);
  client.onMessage(messageHandler); // Set message handler

  Serial.print("Connecting to AWS IOT");
  while (!client.connect(THINGNAME)) {
    Serial.print(".");
    delay(100);
  }

  if(!client.connected()){
    Serial.println("AWS IoT Timeout!");
    return;
  }

  client.subscribe(AWS_IOT_SUBSCRIBE_TOPIC); // Subscribe to a topic
  Serial.println("AWS IoT Connected!");
  //Config current time
  configTime(3600*timezone, daysavetime*3600, "time.nist.gov", "0.pool.ntp.org", "1.pool.ntp.org");
  getLocalTime(&tmstruct, 5000);
  //Change to timestamp format
  sprintf(buffer,"%d-%02d-%02d %02d:%02d:%02d",(tmstruct.tm_year)+1900,( tmstruct.tm_mon)+1, tmstruct.tm_mday,tmstruct.tm_hour , tmstruct.tm_min, tmstruct.tm_sec);
  Serial.println(String(buffer));
}

// Function to publish MQTT message
void publishMessage() {
  //Read sensor data [uncomment this part]
  /*
  voltage = pzem.voltage();
  current = pzem.current();
  power = pzem.power();
  energy = pzem.energy();
  frequency = pzem.frequency();
  pf = pzem.pf();
  */
  
  // Simulate sensor data (replace with actual sensor readings)
  voltage = 218.0 + random() / ((float)RAND_MAX / (221.0 - 218.0));
  current = 0.30 + random() / ((float)RAND_MAX / (0.60 - 0.30));
  power = 75.0 + random() / ((float)RAND_MAX / (80.0 - 75.0));
  energy = 0.04 + random() / ((float)RAND_MAX / (0.07 - 0.04));
  frequency = 49.9 + random() / ((float)RAND_MAX / (50.0 - 49.9));
  pf = 0.85 + random() / ((float)RAND_MAX / (0.99 - 0.85));

     // Prepare JSON payload
    StaticJsonDocument<200> doc;
    doc["sensor"] = "pzem_004T";
    doc["timestamp"] = String(buffer); // Set timestamp to current datetime
    doc["voltage"] = voltage; // Use global variable for voltage
    doc["current"] = current; // Use global variable for current
    doc["power"] = power;     // Use global variable for power
    doc["energy"] = energy;   // Use global variable for energy
    doc["frequency"] = frequency; // Use global variable for frequency
    doc["pf"] = pf;           // Use global variable for power factor
    char jsonBuffer[512];
    serializeJson(doc, jsonBuffer); // Serialize JSON data into a buffer

    // Check if buffer has data
    for (int i = 0; i < BUFFER_SIZE; i++) {
        if (bufferData[i] != "") {
            bufferHasData = true;
            break;
        } else {
            bufferHasData = false;
        }
    }

    // Publish data if connected and buffer has data
    if (client.connected() && bufferHasData) {
        // Display transfer buffer message on M5Stack
        M5.Lcd.setTextColor(YELLOW);
        M5.Lcd.setTextSize(3);
        M5.Lcd.setCursor(0, 0);
        M5.Lcd.print("Transfer Buffer");
        M5.Lcd.setTextColor(WHITE);
        M5.Lcd.setTextSize(1.5);

        // Publish each entry in the buffer
        for (int i = 0; i < BUFFER_SIZE; i++) {
            if (bufferData[i] != "") {
                Serial.print("Publish buffer index: ");
                Serial.println(i);
                client.publish(AWS_IOT_PUBLISH_TOPIC, bufferData[i]);
                bufferData[i] = ""; // Clear the buffer entry after publishing
            }
        }
        bufferIndex = 0; // Clear buffer
        Serial.println("Publish buffer");
    } else if (!client.connected() || WiFi.status() != WL_CONNECTED) {
        // Handle disconnection from AWS IoT or Wi-Fi
        M5.Lcd.fillScreen(BLACK);
        M5.Lcd.setTextColor(RED);
        M5.Lcd.setTextSize(3);
        M5.Lcd.setCursor(0, 0);
        M5.Lcd.print("Disconnected");
        M5.Lcd.setTextColor(WHITE);
        M5.Lcd.setTextSize(1.5);

        // Attempt to reconnect to Wi-Fi
        Serial.println("Attempting to reconnect to AWS IoT...");
        WiFi.mode(WIFI_STA);
        WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
        while (WiFi.status() != WL_CONNECTED) {
            Serial.println("Connecting to Wi-Fi");
            // Display disconnected message on M5Stack
            M5.Lcd.fillScreen(BLACK);
            M5.Lcd.setTextColor(RED);
            M5.Lcd.setTextSize(3);
            M5.Lcd.setCursor(0, 0);
            M5.Lcd.print("Disconnected");
            M5.Lcd.setTextColor(WHITE);
            M5.Lcd.setTextSize(1.5);
            getLocalTime(&tmstruct, 5000);
            sprintf(buffer, "%d-%02d-%02d %02d:%02d:%02d", (tmstruct.tm_year) + 1900, (tmstruct.tm_mon) + 1, tmstruct.tm_mday, tmstruct.tm_hour, tmstruct.tm_min, tmstruct.tm_sec);

            // Simulate sensor data for buffer
            voltage = 218.0 + random() / ((float)RAND_MAX / (221.0 - 218.0));
            current = 0.30 + random() / ((float)RAND_MAX / (0.60 - 0.30));
            power = 75.0 + random() / ((float)RAND_MAX / (80.0 - 75.0));
            energy = 0.04 + random() / ((float)RAND_MAX / (0.07 - 0.04));
            frequency = 49.9 + random() / ((float)RAND_MAX / (50.0 - 49.9));
            pf = 0.85 + random() / ((float)RAND_MAX / (0.99 - 0.85));

            // Prepare JSON payload for buffer data
            StaticJsonDocument<200> doc;
            doc["sensor"] = "pzem_004T";
            doc["timestamp"] = String(buffer); // Set timestamp to current datetime
            doc["voltage"] = voltage; // Use global variable for voltage
            doc["current"] = current; // Use global variable for current
            doc["power"] = power;     // Use global variable for power
            doc["energy"] = energy;   // Use global variable for energy
            doc["frequency"] = frequency; // Use global variable for frequency
            doc["pf"] = pf;           // Use global variable for power factor
            char jsonBuffer[512];
            serializeJson(doc, jsonBuffer); // Serialize JSON data into a buffer
            buffer_data = jsonBuffer; // Create the data string to store

            // Append buffer data to SD card
            bufferData[bufferIndex++] = buffer_data;
            appendBufferToSD();

            // Display disconnect time on M5Stack
            M5.Lcd.setTextColor(RED);
            M5.Lcd.setCursor(0, 90);
            M5.Lcd.print("Disconnect Time: ");
            M5.Lcd.println(String(buffer));

            // Display buffer index on M5Stack
            M5.Lcd.setCursor(0, 100);
            M5.Lcd.print("write buffer index: ");
            M5.Lcd.println(bufferIndex);

            Serial.print("Disconnect time: ");
            Serial.println(buffer);
            Serial.print("write buffer index: ");
            Serial.println(bufferIndex);
            M5.Lcd.setTextColor(WHITE);
            if (bufferIndex >= BUFFER_SIZE) {
                bufferIndex = 0; // Reset index if buffer is full
            }
            delay(500);
        }
        // Configure WiFiClientSecure to use the AWS IoT device credentials
        net.setCACert(AWS_CERT_CA);
        net.setCertificate(AWS_CERT_CRT);
        net.setPrivateKey(AWS_CERT_PRIVATE);
        // Connect to the MQTT broker on the AWS endpoint we defined earlier
        client.begin(AWS_IOT_ENDPOINT, 8883, net);

        // Create a message handler
        client.onMessage(messageHandler);

        // Reconnect to AWS IoT
        if (client.connect(THINGNAME)) {
            Serial.println("Reconnected to AWS IoT");
            client.subscribe(AWS_IOT_SUBSCRIBE_TOPIC);
        } else {
            Serial.println("AWS IoT Reconnection failed.");
        }
    } else {
        // Publish data if connected
        M5.Lcd.setTextColor(GREEN);
        M5.Lcd.setTextSize(3);
        M5.Lcd.setCursor(0, 0);
        M5.Lcd.print("Connected");
        M5.Lcd.setTextColor(WHITE);
        M5.Lcd.setTextSize(1.5);
        client.publish(AWS_IOT_PUBLISH_TOPIC, jsonBuffer);
    }
    read_pzem(); // Read PZEM sensor data
    M5.Lcd.setCursor(0, 90);
    M5.Lcd.print("Time: ");
    M5.Lcd.println(String(buffer)); // Display current time on M5Stack
}

// Function to handle incoming MQTT messages
void messageHandler(String &topic, String &payload) {
  Serial.println("incoming: " + topic + " - " + payload);
  //  StaticJsonDocument<200> doc;
  //  deserializeJson(doc, payload);
  //  const char* message = doc["message"];
}
void read_pzem() {
    // Print the values to the Serial console and LCD
    Serial.print("Voltage: ");
    if (isnan(voltage)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 30);
        M5.Lcd.println("Voltage: NaN");
    } else {
        Serial.print(voltage);
        Serial.println("V");
        M5.Lcd.setCursor(0, 30);
        M5.Lcd.print("Voltage: ");
        M5.Lcd.print(voltage);
        M5.Lcd.println("V");
    }

    Serial.print("Current: ");
    if (isnan(current)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 40);
        M5.Lcd.println("Current: NaN");
    } else {
        Serial.print(current);
        Serial.println("A");
        M5.Lcd.setCursor(0, 40);
        M5.Lcd.print("Current: ");
        M5.Lcd.print(current);
        M5.Lcd.println("A");
    }

    Serial.print("Power: ");
    if (isnan(power)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 50);
        M5.Lcd.println("Power: NaN");
    } else {
        Serial.print(power);
        Serial.println("W");
        M5.Lcd.setCursor(0, 50);
        M5.Lcd.print("Power: ");
        M5.Lcd.print(power);
        M5.Lcd.println("W");
    }
    Serial.print("Energy: ");
    if (isnan(energy)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 60);
        M5.Lcd.println("Energy: NaN");
    } else {
        Serial.print(energy);
        Serial.println("kWh");
        M5.Lcd.setCursor(0, 60);
        M5.Lcd.print("Energy: ");
        M5.Lcd.print(energy);
        M5.Lcd.println("kWh");
    }
     
    Serial.print("Frequency: ");
    if (isnan(frequency)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 70);
        M5.Lcd.println("Frequency: NaN");
    } else {
        Serial.print(frequency, 1);
        Serial.println("Hz");
        M5.Lcd.setCursor(0, 70);
        M5.Lcd.print("Frequency: ");
        M5.Lcd.print(frequency, 1);
        M5.Lcd.println("Hz");
    }

    Serial.print("PF: ");
    if (isnan(pf)) {
        Serial.println("NaN");
        M5.Lcd.setCursor(0, 80);
        M5.Lcd.println("PF: NaN");
    } else {
        Serial.println(pf);
        M5.Lcd.setCursor(0, 80);
        M5.Lcd.print("PF: ");
        M5.Lcd.println(pf);
    }

    delay(50);
}
// Function to recursively list files and directories in a given directory
void listDir(fs::FS &fs, const char * dirname, uint8_t levels){
    // Set cursor position on LCD and print directory name to Serial and LCD
    M5.Lcd.setCursor(0, 10);
    Serial.printf("Listing directory: %s\n", dirname);
    M5.Lcd.printf("Listing directory: %s\n", dirname);

    // Open the directory
    File root = fs.open(dirname);
    if(!root){ // If failed to open directory
        Serial.println("Failed to open directory");
        M5.Lcd.println("Failed to open directory");
        return;
    }
    if(!root.isDirectory()){ // If not a directory
        Serial.println("Not a directory");
        M5.Lcd.println("Not a directory");
        return;
    }

    // Iterate through each file and directory in the directory
    File file = root.openNextFile();
    while(file){
        if(file.isDirectory()){ // If it's a directory
            Serial.print("  DIR : ");
            M5.Lcd.print("  DIR : ");
            Serial.println(file.name()); // Print directory name to Serial
            M5.Lcd.println(file.name()); // Print directory name to LCD
            if(levels){ // If there are more levels to explore
                listDir(fs, file.name(), levels -1); // Recursive call to explore subdirectories
            }
        } else { // If it's a file
            Serial.print("  FILE: ");
            M5.Lcd.print("  FILE: ");
            Serial.print(file.name()); // Print file name to Serial
            M5.Lcd.print(file.name()); // Print file name to LCD
            Serial.print("  SIZE: ");
            M5.Lcd.print("  SIZE: ");
            Serial.println(file.size()); // Print file size to Serial
            M5.Lcd.println(file.size()); // Print file size to LCD
        }
        file = root.openNextFile(); // Move to the next file or directory
    }
}

// Function to read and print the content of a file
void readFile(fs::FS &fs, const char * path) {
    Serial.printf("Reading file: %s\n", path);
    M5.Lcd.printf("Reading file: %s\n", path);

    // Open the file
    File file = fs.open(path);
    if(!file){ // If failed to open file
        Serial.println("Failed to open file for reading");
        M5.Lcd.println("Failed to open file for reading");
        return;
    }

    // Read and print each character from the file
    Serial.print("Read from file: ");
    M5.Lcd.print("Read from file: ");
    while(file.available()){
        int ch = file.read();
        Serial.write(ch); // Print character to Serial
        M5.Lcd.write(ch); // Print character to LCD
    }
}
// Function to write data to a file
void writeFile(fs::FS &fs, const char * path, const char * message){
    Serial.printf("Writing file: %s\n", path);
    //M5.Lcd.printf("Writing file: %s\n", path);

    // Open the file in write mode
    File file = fs.open(path, FILE_WRITE);
    if(!file){ // If failed to open file
        Serial.println("Failed to open file for writing");
       // M5.Lcd.println("Failed to open file for writing");
        return;
    }
    // Write the message to the file
    if(file.print(message)){
        Serial.println("File written"); // Print success message to Serial
      //  M5.Lcd.println("File written");
    } else {
        Serial.println("Write failed"); // Print failure message to Serial
      //  M5.Lcd.println("Write failed");
    }
}

// Function to append data to a file
void appendFile(const char *path, const char * message) {
    M5.Lcd.setCursor(0, 120);
    // Open the file in append mode
    File file = SD.open(path, FILE_APPEND);
    if (!file) { // If failed to open file
        Serial.println("Failed to open file for appending");
        M5.Lcd.println("Failed to open file for appending");
        return;
    }
    // Append the message to the file
    if (file.print(message)) {
        Serial.println("Data appended to file successfully"); // Print success message to Serial
        M5.Lcd.println("Data appended to file successfully");
    } else {
        Serial.println("Append failed"); // Print failure message to Serial
        M5.Lcd.println("Append failed");
    }
    file.close(); // Close the file
}
// Function to append sensor data to a file
void appendSensorData() {
    // Read sensor data
    voltage = pzem.voltage();
    current = pzem.current();
    power = pzem.power();
    energy = pzem.energy();
    frequency = pzem.frequency();
    pf = pzem.pf();
    
    // Get local time
    configTime(3600*timezone, daysavetime*3600, "time.nist.gov", "0.pool.ntp.org", "1.pool.ntp.org");
    getLocalTime(&tmstruct, 5000);
    sprintf(buffer, "%d-%02d-%02d %02d:%02d:%02d", (tmstruct.tm_year) + 1900, (tmstruct.tm_mon) + 1, tmstruct.tm_mday, tmstruct.tm_hour, tmstruct.tm_min, tmstruct.tm_sec);
    
    // Construct CSV data string
    String csvData = String("pzem_004T") + "," + String(buffer) + "," + String(voltage) + "," + String(current) + "," + String(power) + "," + String(energy) + "," + String(frequency) + "," + String(pf) + "\n";
    
    // Append data to file
    appendFile("/data.csv", csvData.c_str());
}

// Function to append buffer data to a file
void appendBufferToSD() {
  // Construct data string from buffer
  String data = String("pzem_004T") + "," + String(buffer) + "," + String(voltage) + "," + String(current) + "," + String(power) + "," + String(energy) + "," + String(frequency) + "," + String(pf)+ "\n";
  
  // Append data to file
  appendFile("/buffer_data.csv", data.c_str());
}

// Function to reconnect to AWS IoT
void reconnectAWS() {
  // Attempt to reconnect to AWS IoT
  Serial.println("Attempting to reconnect to AWS IoT...");
  
  // If reconnected, subscribe to AWS IoT topic
  if (client.connect(THINGNAME)) {
    Serial.println("Reconnected to AWS IoT");
    client.subscribe(AWS_IOT_SUBSCRIBE_TOPIC);
  } else {
    Serial.println("AWS IoT Reconnection failed.");
  }
}

void setup() {
  M5.begin(); // Initialize M5Stack
  M5.Lcd.setBrightness(100); // Set display brightness
  M5.Lcd.fillScreen(BLACK); // Clear screen
  M5.Lcd.setCursor(0, 10); // Set cursor position
  M5.Lcd.setTextColor(WHITE); // Set text color
  M5.Lcd.setTextSize(1.5); // Set text size
  Serial.begin(9600); // Start serial communication
  connectAWS(); // Connect to AWS IoT

  // Initialize time and create header for data file
  configTime(3600*timezone, daysavetime*3600, "time.nist.gov", "0.pool.ntp.org", "1.pool.ntp.org");
  String header = "sensor,timestamp,voltage,current,power,energy,frequency,pf\n";
  writeFile(SD,"/data.csv", header.c_str());
  writeFile(SD,"/buffer_data.csv", header.c_str());
}

void loop() {
  M5.Lcd.fillScreen(BLACK); // Clear screen
  getLocalTime(&tmstruct, 5000); // Get local time
  sprintf(buffer, "%d-%02d-%02d %02d:%02d:%02d", (tmstruct.tm_year) + 1900, (tmstruct.tm_mon) + 1, tmstruct.tm_mday, tmstruct.tm_hour, tmstruct.tm_min, tmstruct.tm_sec); // Format timestamp
  
  // Publish sensor data
  publishMessage();
  
  client.loop(); // MQTT client loop
  
  appendSensorData(); // Append sensor data to file
  delay(50); // Delay for stability
}
