# Fantasy Basketball Analytics Pipeline

## 📊 What Is This?

This project transforms raw fantasy basketball data from a league into actionable insights through an automated data pipeline. Think of it as your fantasy basketball command center - continuously collecting, processing, and analyzing your league's performance data to give you a competitive edge.

## 🧩 How It Works

The pipeline operates like a well-oiled basketball team, with each component playing a specific role:

```
Raw Data → Python Scripts → DBT Transformations → Analytics-Ready Data
```

### Key Components

**1. Data Collection** 🔄
- Automated scripts fetch data from ESPN's fantasy API
- Captures boxscores, player statistics, and team management activities

**2. Data Transformation** ⚙️
- DBT models clean and restructure raw data
- Implements business logic for fantasy scoring
- Maintains data quality through automated testing
- Creates a consistent, reliable single source of truth

**3. Analysis Layer** 📈
- Rolling statistical trends (7/15/30 days)
- Category strength analysis
- Matchup projections
- Waiver wire opportunity identification

## 🚀 Getting Started

### Prerequisites

```
Python 3.10+
PostgreSQL
DBT Core
ESPN Fantasy Basketball account
```

### Quick Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/fantasy-basketball-pipeline.git
   cd fantasy-basketball-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials**
   ```bash
   # Create .env file
   cp example.env .env
   
   # Add your credentials
   # ESPN_S2=your_espn_s2_cookie
   # SWID=your_swid_cookie
   ```

4. **Initialize the database**
   ```bash
   dbt deps
   dbt seed
   ```

5. **Run the pipeline**
   ```bash
   python scripts/run_pipeline.py
   ```

## 📁 Project Structure

```
fantasy-basketball-pipeline/
├── src/                     # Python data collectors
├── models/                  # DBT transformation models
│   ├── sources/             # Raw data specifications
│   ├── silver/              # Cleaned, typed data
│   └── gold/                # Business-specific views
```

## 🔄 Pipeline Overview

The process flows like a fast break:

1. **Ingest**: Python scripts connect to ESPN's API to extract fresh data
2. **Transform**: DBT models clean, validate, and restructure the data
3. **Analyze**: New calculations and aggregations generate insights
4. **Deliver**: Results are stored in analytics-ready tables

Like any good basketball team, each component has clear responsibilities while working together toward a common goal.

## 📊 Key Metrics

This pipeline helps you track:

- **Player Performance Trends**
- **Category Strengths**: Where your team excels vs. the competition
- **Weekly Matchup Analytics**: Projected category outcomes
- **Transaction Impact Analysis**: How roster moves affect team balance

## 🛠️ Development

### Testing

```bash
# Run DBT tests
dbt test
```

### Adding New Features

1. Create a feature branch
2. Implement changes
3. Write tests
4. Submit a pull request

## 🤝 Contributing

Contributions make this project better! Whether it's:
- Adding new analytical models
- Improving data collection efficiency
- Enhancing documentation
- Fixing bugs

Please feel free to fork, modify, and make pull requests.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

*Remember, fantasy basketball is a marathon, not a sprint. This pipeline helps you make data-driven decisions throughout the season, not just on draft day.*