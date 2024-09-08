# Spark Online Retail Project

## Overview
This project analyzes the UCI Online Retail dataset using Apache Spark for data cleaning, aggregation, and transformation.

## Setup Instructions
1. Clone the repository.
2. Install required packages:
    ```bash
    pip install -r requirements.txt
    ```
3. Place the dataset (`OnlineRetail.csv`) inside the `data/` directory.
4. Run the Spark application:
    ```bash
    python src/spark_app.py
    ```

## Instructions to run tests 
    ```bash
    python3 -m venv venv  
    source venv/bin/activate 
    pip install -r requirements.txt
    pytest tests/test_aggregations.py
    pytest tests/test_aggregations_purchases.py
    deactivate
    ```

## File Structure
- `src/spark_app.py`: Main entry point for the Spark application.
- `src/data_cleaning.py`: Contains data cleaning functions.
- `src/aggregations.py`: Contains aggregation and transformation functions.
- `tests/`: Unit tests for data cleaning and transformations.
