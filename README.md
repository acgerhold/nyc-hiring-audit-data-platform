# nyc-hiring-audit-data-platform
Real-time event-driven data platform for auditing NYC government hiring practices using Kafka, DuckDB lakehouse architecture, and fuzzy string matching. Features automated ingestion, multi-layer data processing, and REST API for audit insights.

## 🏗️ Architecture Overview
- **Event Streaming**: Apache Kafka for real-time data processing
- **Lakehouse**: DuckDB bronze/silver/gold layer architecture  
- **String Matching**: Advanced fuzzy matching with rapidfuzz
- **API**: FastAPI REST endpoints for audit insights
- **Orchestration**: Event-driven processing pipeline

## 🚀 Key Features
- Real-time file processing with Kafka event streaming
- Multi-source data integration (CSV, Excel, APIs)
- Scalable DuckDB lakehouse with medallion architecture
- Advanced fuzzy string matching for data reconciliation
- RESTful API for audit query and validation
- Comprehensive logging and error handling

## 📊 Business Value
- Automated detection of hiring discrepancies
- Salary benchmarking against market data
- Job posting duration analysis
- Match ratio scoring for data quality assessment 
