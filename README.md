# 🎬 YouTube Channel Analytics — Taylor Swift (Regular Music Videos, 2020–2025)

An end-to-end analytics project examining Taylor Swift’s YouTube channel performance using the **YouTube Data API**, **MySQL**, and **Tableau**.  
The analysis focuses on Regular (long-form) music videos between 2020 and 2025, uncovering patterns in views, engagement, and content strategy.

---

## 📁 Project Structure

<pre>
  youtube_analysis/
  │
  ├─ data/ → raw & cleaned data files
  │ ├─ cleaned_data/
  │ │ └─ youtube_analytics_export.xlsx # MySQL-exported views for Tableau
  │ ├─ ts_channel_data.csv # channel-level metrics
  │ └─ ts_video_data.csv # per-video metrics
  │ 
  ├─ notebooks/ → Jupyter notebooks for API & export
  │ ├─ export.ipynb
  │ └─ youtube_api_analytics.ipynb
  │ 
  ├─ sql/ → schema, calculations, and views
  │ └─ yt_analytics.sql
  │ 
  ├─ visualizations/ → Tableau workbook
  │  └─ yt_dashboard.twb
  │
  ├─ .gitattributes
  │
  ├─ LICENSE → MIT license
  │
  └─ README.md → project documentation
</pre>

---

## 🧭 Workflow Overview

1. **Data Extraction**
   - Pulled data from the **YouTube Data API** (channel, playlist, video endpoints).  
   - Saved raw results into `ts_channel_data.csv` and `ts_video_data.csv`.

2. **Data Modeling (SQL)**
   - Designed schema and analytical views in [`sql/yt_analytics.sql`](sql/yt_analytics.sql).
   - Computed KPIs, monthly summaries, correlations, and engagement metrics.

  3. **Data Cleaning (Python)**
   - Processed and merged datasets in Jupyter Notebook.  
   - Exported clean, analysis-ready workbook: [`data/cleaned_data/youtube_analytics_export.xlsx`](data/cleaned_data/youtube_analytics_export.xlsx)

4. **Visualization (Tableau)**
   - Built an interactive dashboard in [`visualizations/yt_dashboard.twb`](visualizations/yt_dashboard.twb).  
   - Combined KPI, trend, scatter, correlation, and Pareto analyses for insight storytelling.

---

## 📊 Tableau Dashboard Highlights

### 1️⃣ KPI Overview
**Metrics:** Total videos, total views, median views, average engagement rate.  
> *Summarizes 2020–2025 performance and channel health at a glance.*

### 2️⃣ Monthly Trend — Views vs. Engagement
**Type:** Dual-axis bar + line chart  
> Reveals cyclical performance patterns around album releases and major content drops.

### 3️⃣ Correlation Heatmap
**Type:** Diverging color matrix (–1 to +1)  
> Quantifies relationships among metrics (Views, Likes, Comments, Duration, Engagement).  
> High correlation (r ≈ 0.9) between Views ↔ Likes indicates a strong fanbase response pattern.

### 4️⃣ Duration vs Views Scatter
**Type:** Log–log scatter  
> Identifies the optimal video length range (4–6 minutes) for balanced reach and engagement.

### 5️⃣ Engagement vs Views Bubble Chart
**Type:** Quadrant bubble chart  
> Segments Regular videos into “Breakout Hits,” “Hidden Gems,” “Viral but Shallow,” and “Underperformers.”  
> Enables strategic focus on high-engagement formats.

### 6️⃣ Pareto Chart
**Type:** Dual-axis bar + cumulative line  
> Confirms the 80/20 rule: ~20% of Regular videos generate ~80% of total views.  
> Supports decisions on where to reinvest creative and marketing effort.

---

## 💡 Key Insights (2020–2025)

| Theme | Finding | Strategic Takeaway |
|-------|----------|-------------------|
| **Optimal Length** | Videos around **4–6 minutes** achieve the best view-to-engagement balance. | Maintain narrative storytelling within this range. |
| **Engagement Dynamics** | Engagement remains stable even during high-volume release periods. | Leverage fan loyalty through follow-up content cycles. |
| **Portfolio Concentration** | ~20% of Regular videos deliver ~80% of views. | Prioritize re-use of proven creative formats. |
| **Correlation** | Views and likes move almost perfectly together (r ≈ 0.9); duration correlation weak (–0.25). | Focus more on content quality & emotion than length. |

---

## 🧠 Tech Stack

| Layer | Tools |
|-------|-------|
| Data Extraction | YouTube Data API (Python, Pandas, Google API Client) |
| Data Storage | MySQL 8 |
| Data Modeling | SQL Views, KPIs, Pearson Correlation |
| Data Visualization | Tableau Desktop |
| Version Control | Git + GitHub |
| Environment | Jupyter Notebook |

---

## 🚀 Reproducibility

```bash
# Clone the repo
git clone https://github.com/quanh171/youtube_analysis.git
cd youtube_analysis

# (Optional) run SQL locally
mysql -u root -p < sql/yt_analytics.sql

