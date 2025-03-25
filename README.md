# Enhanced Satellite Control with Real-Time LSTM Modeling (SGP4 BOOST)

This project leverages **NOAA data** and a TLE-based **LSTM** model to provide **real-time predictive analytics** for **enhanced space surveillance and management**. By learning on-the-fly, it aims to **improve satellite control** and decision-making, all while running efficiently on **AWS** for scalable performance.

---

## Table of Contents
- [About](#about)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Backend (AWS and Model Training)](#backend-aws-and-model-training)
- [Frontend](#frontend)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)

---

## About

Using **NOAA** data, **TLE** data, and an **LSTM** model, this project performs **real-time learning** to provide predictive analytics that help manage and optimize satellite operations. Hosted on AWS, it ensures robust scalability, real-time processing, and reliable access to external data feeds.

---

## Key Features

1. **Real-Time Learning**  
   The model continuously incorporates new TLE and NOAA data to update its predictions, minimizing latency in decision-making.

2. **AWS-Hosted**  
   Leverages AWS services (e.g., EC2, Lambda, or SageMaker) for efficient scaling and fault tolerance.

3. **Predictive Analytics**  
   Harnesses LSTM-based modeling to forecast satellite positions and behaviors with high precision.

4. **Enhanced Satellite Control**  
   Optimizes flight paths, collision avoidance, and resource allocation by integrating real-time analytics into control systems.

---

## Architecture

```
                 +-----------+         +----------------+         +-----------+
NOAA Data  --->  | Data Ingest| ---->  |   LSTM Model   | ---->   |  Predictions
TLE Data   --->  | AWS S3/ RDS|        |   (AWS)        |         |  & Control
                 +-----------+         +----------------+         +-----------+
                      ^                       ^                       |
                      |                       |                       |
                   (DataCleaning, etc.)   (Model Training)        (Frontend)
                      |                       |
                      +---------Backend-------+
```

### Backend (AWS and Model Training)

Files like:

- **DataCleaning.py**  
  Cleans and normalizes NOAA + TLE data before feeding into the model.

- **OMNI_PARSER.py**  
  Parses OMNI/NOAA data streams for relevant parameters.

- **TLE_Fetch.py**  
  Periodically fetches the latest TLE data and stores it for processing.

- **cookies.pkl**  
  Holds session or caching info (if needed) during data fetch or authentication.

- **data_input.py / database.py / omni_input.py**  
  Manages interactions with the data store (local or AWS-based), does real-time updates, and read/write operations for the model.

### Frontend

The frontend (web or GUI) consumes the predictions via an API or direct AWS endpoint. It might include real-time visualization of satellite orbits, dashboards with relevant metrics, and controls for operators to intervene or update parameters.

---

## Setup and Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your_username/your_project.git
   cd your_project
   ```

2. **Install Python Requirements**:
   ```bash
   pip install -r requirements.txt
   ```
   (Make sure you have Python 3.8+)

3. **AWS Configuration**:
   - Ensure you have an AWS account with permissions for S3, EC2, or SageMaker.
   - Configure AWS CLI with `aws configure`.

---

## Usage

1. **Ingest and Clean Data**  
   - Run `DataCleaning.py` or similar scripts to cleanse and normalize NOAA + TLE data.

2. **Train the LSTM Model**  
   - Use AWS compute (e.g., EC2, SageMaker) or local GPU to train the LSTM model.  
   - Adjust hyperparameters in a config file or script.

3. **Deploy**  
   - Deploy the model on AWS (e.g., SageMaker endpoint, EC2, or Lambda for small models).  
   - Ensure your `TLE_Fetch.py` is scheduled (e.g., via Cron, AWS Lambda, or a Docker container).

4. **Frontend Access**  
   - Launch your web or GUI app to visualize satellite orbits, track predicted states, and perform control actions as needed.

---

## Future Enhancements

- **Automatic Collision Avoidance**: Integrate collision detection algorithms into the LSTM pipeline.  
- **Multi-Satellite Coordination**: Scale up the model to coordinate entire constellations.  
- **Extended Data Sources**: Incorporate additional data streams (e.g., space weather, solar flux) for even more accurate predictions.

---

## Contributing

1. **Fork the Repo**  
2. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/my-feature
   ```
3. **Commit and Push**:
   ```bash
   git commit -m "Add feature"
   git push origin feature/my-feature
   ```
4. **Open a Pull Request** on GitHub.

---

## License

This project is licensed under the [MIT License](LICENSE).

---
