package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// SensorData represents the sensor readings
type SensorData struct {
	ID          string    `json:"id,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
	Pressure    float64   `json:"pressure"`
	Altitude    float64   `json:"altitude"`
	Location    string    `json:"location"`
}

// Response represents API response
type Response struct {
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// App contains the application dependencies
type App struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
	bucket   string
	org      string
}

func main() {
	// Environment variables
	influxURL := getEnv("INFLUXDB_URL", "http://influxdb:8086")
	influxToken := getEnv("INFLUXDB_TOKEN", "")
	influxOrg := getEnv("INFLUXDB_ORG", "myorg")
	influxBucket := getEnv("INFLUXDB_BUCKET", "sensor_data")
	port := getEnv("PORT", "8080")

	if influxToken == "" {
		log.Fatal("INFLUXDB_TOKEN environment variable is required")
	}

	// Create InfluxDB client
	client := influxdb2.NewClient(influxURL, influxToken)
	defer client.Close()

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	health, err := client.Health(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to InfluxDB: %v", err)
	}
	if health.Status != "pass" {
		log.Fatalf("InfluxDB health check failed: %s", health.Status)
	}

	log.Println("Successfully connected to InfluxDB")

	// Initialize app
	app := &App{
		client:   client,
		writeAPI: client.WriteAPIBlocking(influxOrg, influxBucket),
		queryAPI: client.QueryAPI(influxOrg),
		bucket:   influxBucket,
		org:      influxOrg,
	}

	// Setup routes
	r := mux.NewRouter()
	
	// API routes
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/sensor-data", app.createSensorData).Methods("POST")
	api.HandleFunc("/sensor-data", app.getSensorData).Methods("GET")
	api.HandleFunc("/sensor-data/{id}", app.getSensorDataByID).Methods("GET")
	api.HandleFunc("/sensor-data/{id}", app.updateSensorData).Methods("PUT")
	api.HandleFunc("/sensor-data/{id}", app.deleteSensorData).Methods("DELETE")
	
	// Health check
	r.HandleFunc("/health", app.healthCheck).Methods("GET")

	r.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "API is working",
	})
}).Methods("GET")
	
	// CORS middleware
	r.Use(corsMiddleware)

	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func (app *App) createSensorData(w http.ResponseWriter, r *http.Request) {
	var data SensorData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON format", err)
		return
	}

	// Set timestamp if not provided
	if data.Timestamp.IsZero() {
		data.Timestamp = time.Now()
	}

	// Generate ID if not provided
	if data.ID == "" {
		data.ID = fmt.Sprintf("sensor_%d", data.Timestamp.Unix())
	}

	// Create InfluxDB point
	p := influxdb2.NewPointWithMeasurement("sensor_readings").
		AddTag("sensor_id", data.ID).
		AddTag("location", data.Location).
		AddField("temperature", data.Temperature).
		AddField("humidity", data.Humidity).
		AddField("pressure", data.Pressure).
		AddField("altitude", data.Altitude).
		SetTime(data.Timestamp)

	// Write to InfluxDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := app.writeAPI.WritePoint(ctx, p); err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to write to database", err)
		return
	}

	sendSuccessResponse(w, http.StatusCreated, "Sensor data created successfully", data)
}

func (app *App) getSensorData(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	limit := r.URL.Query().Get("limit")
	location := r.URL.Query().Get("location")
	
	limitInt := 100 // default limit
	if limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 {
			limitInt = l
		}
	}

	// Build query
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -24h)
		|> filter(fn: (r) => r._measurement == "sensor_readings")
	`, app.bucket)

	if location != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r.location == "%s")`, location)
	}

	query += fmt.Sprintf(`
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> limit(n: %d)
		|> sort(columns: ["_time"], desc: true)
	`, limitInt)

	// Execute query
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	result, err := app.queryAPI.Query(ctx, query)
	if err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to query database", err)
		return
	}

	var sensorData []SensorData
	for result.Next() {
		record := result.Record()
		
		data := SensorData{
			ID:        record.ValueByKey("sensor_id").(string),
			Timestamp: record.Time(),
			Location:  record.ValueByKey("location").(string),
		}

		// Safely extract field values
		if temp := record.ValueByKey("temperature"); temp != nil {
			data.Temperature = temp.(float64)
		}
		if humidity := record.ValueByKey("humidity"); humidity != nil {
			data.Humidity = humidity.(float64)
		}
		if pressure := record.ValueByKey("pressure"); pressure != nil {
			data.Pressure = pressure.(float64)
		}
		if altitude := record.ValueByKey("altitude"); altitude != nil {
			data.Altitude = altitude.(float64)
		}

		sensorData = append(sensorData, data)
	}

	if result.Err() != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Error processing query results", result.Err())
		return
	}

	sendSuccessResponse(w, http.StatusOK, "Data retrieved successfully", sensorData)
}

func (app *App) getSensorDataByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -30d)
		|> filter(fn: (r) => r._measurement == "sensor_readings")
		|> filter(fn: (r) => r.sensor_id == "%s")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> sort(columns: ["_time"], desc: true)
		|> limit(n: 1)
	`, app.bucket, id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	result, err := app.queryAPI.Query(ctx, query)
	if err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to query database", err)
		return
	}

	if !result.Next() {
		sendErrorResponse(w, http.StatusNotFound, "Sensor data not found", nil)
		return
	}

	record := result.Record()
	data := SensorData{
		ID:        record.ValueByKey("sensor_id").(string),
		Timestamp: record.Time(),
		Location:  record.ValueByKey("location").(string),
	}

	// Safely extract field values
	if temp := record.ValueByKey("temperature"); temp != nil {
		data.Temperature = temp.(float64)
	}
	if humidity := record.ValueByKey("humidity"); humidity != nil {
		data.Humidity = humidity.(float64)
	}
	if pressure := record.ValueByKey("pressure"); pressure != nil {
		data.Pressure = pressure.(float64)
	}
	if altitude := record.ValueByKey("altitude"); altitude != nil {
		data.Altitude = altitude.(float64)
	}

	sendSuccessResponse(w, http.StatusOK, "Data retrieved successfully", data)
}

func (app *App) updateSensorData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var updateData SensorData
	if err := json.NewDecoder(r.Body).Decode(&updateData); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON format", err)
		return
	}

	// Set ID and timestamp
	updateData.ID = id
	if updateData.Timestamp.IsZero() {
		updateData.Timestamp = time.Now()
	}

	// Create new point (InfluxDB is immutable, so we add a new point)
	p := influxdb2.NewPointWithMeasurement("sensor_readings").
		AddTag("sensor_id", updateData.ID).
		AddTag("location", updateData.Location).
		AddField("temperature", updateData.Temperature).
		AddField("humidity", updateData.Humidity).
		AddField("pressure", updateData.Pressure).
		AddField("altitude", updateData.Altitude).
		SetTime(updateData.Timestamp)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := app.writeAPI.WritePoint(ctx, p); err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to update data", err)
		return
	}

	sendSuccessResponse(w, http.StatusOK, "Sensor data updated successfully", updateData)
}

func (app *App) deleteSensorData(w http.ResponseWriter, r *http.Request) {
	// Note: InfluxDB doesn't support traditional delete operations
	// In a real-world scenario, you might mark records as deleted or use retention policies
	sendErrorResponse(w, http.StatusNotImplemented, "Delete operation not implemented for time series data", nil)
}

func (app *App) healthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	health, err := app.client.Health(ctx)
	if err != nil || health.Status != "pass" {
		sendErrorResponse(w, http.StatusServiceUnavailable, "Database connection failed", err)
		return
	}

	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"database":  "connected",
	}
	
	sendSuccessResponse(w, http.StatusOK, "Service is healthy", response)
}

// Utility functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func sendSuccessResponse(w http.ResponseWriter, statusCode int, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	response := Response{
		Status:  "success",
		Message: message,
		Data:    data,
	}
	
	json.NewEncoder(w).Encode(response)
}

func sendErrorResponse(w http.ResponseWriter, statusCode int, message string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	response := Response{
		Status:  "error",
		Message: message,
	}
	
	if err != nil {
		log.Printf("Error: %v", err)
	}
	
	json.NewEncoder(w).Encode(response)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		next.ServeHTTP(w, r)
	})
}