"""
AirStay Analytics Dashboard
Interactive analytics dashboard for property hosts and administrators.
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# Page config
st.set_page_config(
    page_title="AirStay Analytics",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
API_BASE_URL = os.getenv("API_URL", "http://api:8000") + "/api/v1"
DB_CONFIG = {
    'host': os.getenv("POSTGRES_HOST", "postgres"),
    'database': os.getenv("POSTGRES_DB", "airstay_db"),
    'user': os.getenv("POSTGRES_USER", "airstay"),
    'password': os.getenv("POSTGRES_PASSWORD", "airstay_pass"),
    'port': int(os.getenv("POSTGRES_PORT", 5432))
}


@st.cache_resource
def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def run_query(query, params=None):
    """Execute a query and return a DataFrame."""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database query error: {e}")
        st.cache_resource.clear()
        return pd.DataFrame()


def api_get(endpoint, params=None):
    """Make GET request to API."""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


# Sidebar
st.sidebar.title("AirStay Analytics")
st.sidebar.markdown("---")

view_mode = st.sidebar.radio(
    "Select View",
    [
        "Platform Overview",
        "Property Performance",
        "City Analysis",
        "Revenue Analytics",
        "Guest Insights",
        "Pricing Intelligence",
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

date_range = st.sidebar.selectbox(
    "Time Period",
    ["Last 7 days", "Last 30 days", "Last 90 days", "Last 180 days", "Custom"]
)

if date_range == "Custom":
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("Start", datetime.now() - timedelta(days=30))
    end_date = col2.date_input("End", datetime.now())
else:
    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 90 days": 90,
        "Last 180 days": 180,
    }
    days = days_map[date_range]
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)


# ==========================================
# Platform Overview
# ==========================================
if view_mode == "Platform Overview":
    st.title("Platform Overview")
    st.caption(f"Data from {start_date} to {end_date}")

    summary_df = run_query("""
        SELECT
            COUNT(DISTINCT b.booking_id) as total_bookings,
            COALESCE(SUM(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as total_revenue,
            COALESCE(AVG(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as avg_booking_value,
            COUNT(DISTINCT b.guest_id) as unique_guests,
            COUNT(DISTINCT b.property_id) as active_properties,
            COALESCE(AVG(b.nights), 0) as avg_nights
        FROM silver.bookings b
        WHERE b.created_at >= %s AND b.created_at <= %s
            AND b.booking_status IN ('confirmed', 'completed')
    """, (start_date, end_date))

    properties_count = run_query("SELECT COUNT(*) as total FROM silver.properties WHERE is_active = TRUE")
    reviews_count = run_query("SELECT COUNT(*) as total FROM silver.reviews")

    if not summary_df.empty:
        row = summary_df.iloc[0]
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Total Bookings", f"{int(row['total_bookings']):,}")
        col2.metric("Revenue", f"${float(row['total_revenue']):,.0f}")
        col3.metric("Avg Booking", f"${float(row['avg_booking_value']):,.0f}")
        col4.metric("Unique Guests", f"{int(row['unique_guests']):,}")
        col5.metric("Active Properties", f"{int(properties_count.iloc[0]['total']):,}")
        col6.metric("Total Reviews", f"{int(reviews_count.iloc[0]['total']):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Daily Bookings and Revenue")
        trends_df = run_query("""
            SELECT
                DATE(created_at) as date,
                COUNT(*) as bookings,
                SUM(total_price) as revenue
            FROM silver.bookings
            WHERE created_at >= %s AND created_at <= %s
                AND booking_status IN ('confirmed', 'completed')
            GROUP BY DATE(created_at)
            ORDER BY date
        """, (start_date, end_date))

        if not trends_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=trends_df['date'], y=trends_df['bookings'],
                name='Bookings', marker_color='#1f77b4'
            ))
            fig.add_trace(go.Scatter(
                x=trends_df['date'], y=trends_df['revenue'],
                name='Revenue ($)', yaxis='y2',
                line=dict(color='#2ca02c', width=2)
            ))
            fig.update_layout(
                yaxis_title='Bookings', height=350,
                yaxis2=dict(title='Revenue ($)', overlaying='y', side='right'),
                hovermode='x unified', legend=dict(orientation='h', y=1.12)
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Bookings by City")
        cities_df = run_query("""
            SELECT p.location_city as city, COUNT(*) as bookings,
                SUM(b.total_price) as revenue
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE b.created_at >= %s AND b.created_at <= %s
                AND b.booking_status IN ('confirmed', 'completed')
            GROUP BY p.location_city ORDER BY bookings DESC
        """, (start_date, end_date))

        if not cities_df.empty:
            fig = px.bar(
                cities_df, x='city', y='bookings',
                color='revenue', color_continuous_scale='Blues',
                title=None
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Property Type Distribution")
        type_df = run_query("""
            SELECT property_type, COUNT(*) as count
            FROM silver.properties WHERE is_active = TRUE
            GROUP BY property_type ORDER BY count DESC
        """)
        if not type_df.empty:
            fig = px.pie(type_df, values='count', names='property_type', hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Booking Status Breakdown")
        status_df = run_query("""
            SELECT booking_status, COUNT(*) as count
            FROM silver.bookings
            WHERE created_at >= %s AND created_at <= %s
            GROUP BY booking_status
        """, (start_date, end_date))
        if not status_df.empty:
            colors = {'confirmed': '#2ca02c', 'completed': '#1f77b4', 'canceled': '#d62728'}
            fig = px.pie(
                status_df, values='count', names='booking_status',
                color='booking_status', color_discrete_map=colors, hole=0.4
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


# ==========================================
# Property Performance
# ==========================================
elif view_mode == "Property Performance":
    st.title("Property Performance")

    props_df = run_query("""
        SELECT property_id, listing_id, title, location_city
        FROM silver.properties WHERE is_active = TRUE
        ORDER BY property_id LIMIT 200
    """)

    if not props_df.empty:
        selected = st.selectbox(
            "Select Property",
            options=props_df['property_id'].tolist(),
            format_func=lambda x: f"{props_df[props_df['property_id']==x]['listing_id'].values[0]} - {props_df[props_df['property_id']==x]['title'].values[0]}"
        )

        if selected:
            perf_df = run_query("""
                SELECT * FROM gold.agg_property_performance
                WHERE property_id = %s AND metric_date >= %s AND metric_date <= %s
                ORDER BY metric_date
            """, (selected, start_date, end_date))

            if not perf_df.empty:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Bookings", f"{perf_df['bookings_count'].sum():,}")
                col2.metric("Total Revenue", f"${perf_df['revenue_total'].sum():,.0f}")
                col3.metric("Avg Occupancy", f"{perf_df['occupancy_rate'].mean():.1f}%")
                col4.metric("Avg Rating", f"{perf_df['avg_rating'].mean():.2f}/5.0")

                st.markdown("---")
                col1, col2 = st.columns(2)

                with col1:
                    fig = px.line(perf_df, x='metric_date', y='revenue_total',
                                  title='Daily Revenue', markers=True)
                    fig.update_traces(line_color='#2ca02c')
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    fig = px.line(perf_df, x='metric_date', y='occupancy_rate',
                                  title='Occupancy Rate (%)', markers=True)
                    fig.update_traces(line_color='#ff7f0e')
                    st.plotly_chart(fig, use_container_width=True)

                st.subheader("Detailed Metrics")
                st.dataframe(
                    perf_df[['metric_date', 'bookings_count', 'revenue_total',
                            'occupancy_rate', 'avg_nightly_rate', 'avg_rating']],
                    use_container_width=True
                )
            else:
                st.info("No performance data available for the selected period.")

            st.subheader("Recent Reviews")
            reviews_df = run_query("""
                SELECT rating, review_text, sentiment_label, created_at
                FROM silver.reviews WHERE property_id = %s
                ORDER BY created_at DESC LIMIT 10
            """, (selected,))
            if not reviews_df.empty:
                st.dataframe(reviews_df, use_container_width=True)


# ==========================================
# City Analysis
# ==========================================
elif view_mode == "City Analysis":
    st.title("City Analysis")

    cities = run_query("""
        SELECT DISTINCT location_city as city
        FROM silver.properties WHERE is_active = TRUE ORDER BY city
    """)

    if not cities.empty:
        selected_city = st.selectbox("Select City", cities['city'].tolist())

        if selected_city:
            city_data = run_query("""
                SELECT * FROM gold.agg_city_metrics
                WHERE city = %s AND metric_date >= %s AND metric_date <= %s
                ORDER BY metric_date
            """, (selected_city, start_date, end_date))

            if not city_data.empty:
                col1, col2, col3 = st.columns(3)
                col1.metric("Active Properties", f"{city_data['active_properties'].iloc[-1]:,}")
                col2.metric("Total Bookings", f"{city_data['total_bookings'].sum():,}")
                col3.metric("Total Revenue", f"${city_data['total_revenue'].sum():,.0f}")

                st.markdown("---")
                col1, col2 = st.columns(2)

                with col1:
                    fig = px.area(city_data, x='metric_date', y='total_bookings',
                                  title=f'Daily Bookings in {selected_city}')
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    fig = px.line(city_data, x='metric_date', y='avg_nightly_rate',
                                  title='Average Nightly Rate', markers=True)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No metrics data for this city in the selected period.")

            st.subheader(f"Properties in {selected_city}")
            city_props = run_query("""
                SELECT property_type, COUNT(*) as count,
                    ROUND(AVG(base_price)::DECIMAL, 2) as avg_price,
                    ROUND(AVG(property_rating)::DECIMAL, 2) as avg_rating
                FROM silver.properties
                WHERE location_city = %s AND is_active = TRUE
                GROUP BY property_type ORDER BY count DESC
            """, (selected_city,))
            if not city_props.empty:
                st.dataframe(city_props, use_container_width=True)


# ==========================================
# Revenue Analytics
# ==========================================
elif view_mode == "Revenue Analytics":
    st.title("Revenue Analytics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Channel")
        channel_df = run_query("""
            SELECT booking_channel, COUNT(*) as bookings,
                SUM(total_price) as revenue
            FROM silver.bookings
            WHERE created_at >= %s AND created_at <= %s
                AND booking_status IN ('confirmed', 'completed')
            GROUP BY booking_channel ORDER BY revenue DESC
        """, (start_date, end_date))

        if not channel_df.empty:
            fig = px.bar(channel_df, x='booking_channel', y='revenue',
                         color='bookings', title=None)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Revenue by Property Type")
        rev_type_df = run_query("""
            SELECT p.property_type, SUM(b.total_price) as revenue,
                COUNT(*) as bookings
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE b.created_at >= %s AND b.created_at <= %s
                AND b.booking_status IN ('confirmed', 'completed')
            GROUP BY p.property_type ORDER BY revenue DESC
        """, (start_date, end_date))

        if not rev_type_df.empty:
            fig = px.pie(rev_type_df, values='revenue', names='property_type', hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 20 Revenue Properties")
    top_props = run_query("""
        SELECT p.listing_id, p.title, p.location_city,
            COUNT(b.booking_id) as bookings,
            SUM(b.total_price) as total_revenue,
            ROUND(AVG(b.total_price)::DECIMAL, 2) as avg_revenue
        FROM silver.bookings b
        JOIN silver.properties p ON b.property_id = p.property_id
        WHERE b.created_at >= %s AND b.created_at <= %s
            AND b.booking_status IN ('confirmed', 'completed')
        GROUP BY p.listing_id, p.title, p.location_city
        ORDER BY total_revenue DESC LIMIT 20
    """, (start_date, end_date))

    if not top_props.empty:
        st.dataframe(top_props, use_container_width=True)

    st.subheader("Monthly Revenue Trend")
    monthly_df = run_query("""
        SELECT
            DATE_TRUNC('month', created_at)::DATE as month,
            SUM(total_price) as revenue,
            COUNT(*) as bookings
        FROM silver.bookings
        WHERE booking_status IN ('confirmed', 'completed')
            AND created_at >= %s AND created_at <= %s
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
    """, (start_date, end_date))

    if not monthly_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly_df['month'], y=monthly_df['revenue'],
                             name='Revenue', marker_color='#2ca02c'))
        fig.update_layout(height=350, yaxis_title='Revenue ($)')
        st.plotly_chart(fig, use_container_width=True)


# ==========================================
# Guest Insights
# ==========================================
elif view_mode == "Guest Insights":
    st.title("Guest Insights")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Stay Duration Distribution")
        stay_df = run_query("""
            SELECT nights, COUNT(*) as count
            FROM silver.bookings
            WHERE created_at >= %s AND created_at <= %s
                AND booking_status IN ('confirmed', 'completed')
                AND nights IS NOT NULL
            GROUP BY nights ORDER BY nights
        """, (start_date, end_date))

        if not stay_df.empty:
            fig = px.bar(stay_df, x='nights', y='count',
                         title=None, labels={'nights': 'Nights', 'count': 'Bookings'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Guest Count Distribution")
        guest_df = run_query("""
            SELECT num_guests, COUNT(*) as count
            FROM silver.bookings
            WHERE created_at >= %s AND created_at <= %s
                AND booking_status IN ('confirmed', 'completed')
            GROUP BY num_guests ORDER BY num_guests
        """, (start_date, end_date))

        if not guest_df.empty:
            fig = px.bar(guest_df, x='num_guests', y='count',
                         title=None, labels={'num_guests': 'Guests', 'count': 'Bookings'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Review Ratings Distribution")
    ratings_df = run_query("""
        SELECT rating, COUNT(*) as count
        FROM silver.reviews
        GROUP BY rating ORDER BY rating
    """)

    if not ratings_df.empty:
        fig = px.bar(ratings_df, x='rating', y='count',
                     color='rating', color_continuous_scale='RdYlGn',
                     labels={'rating': 'Rating (1-5)', 'count': 'Reviews'})
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Sentiment Breakdown")
        sentiment_df = run_query("""
            SELECT sentiment_label, COUNT(*) as count
            FROM silver.reviews WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """)
        if not sentiment_df.empty:
            colors = {'positive': '#2ca02c', 'neutral': '#ff7f0e', 'negative': '#d62728'}
            fig = px.pie(sentiment_df, values='count', names='sentiment_label',
                         color='sentiment_label', color_discrete_map=colors, hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Rating by City")
        city_rating_df = run_query("""
            SELECT p.location_city as city,
                ROUND(AVG(r.rating)::DECIMAL, 2) as avg_rating,
                COUNT(r.review_id) as review_count
            FROM silver.reviews r
            JOIN silver.properties p ON r.property_id = p.property_id
            GROUP BY p.location_city ORDER BY avg_rating DESC
        """)
        if not city_rating_df.empty:
            fig = px.bar(city_rating_df, x='city', y='avg_rating',
                         color='review_count', title=None)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


# ==========================================
# Pricing Intelligence
# ==========================================
elif view_mode == "Pricing Intelligence":
    st.title("Pricing Intelligence")

    st.subheader("Price Recommendations")
    recs_df = run_query("""
        SELECT pr.property_id, p.title, p.location_city,
            pr.current_price, pr.recommended_price,
            pr.price_change_pct, pr.reason, pr.confidence_score
        FROM gold.pricing_recommendations pr
        JOIN silver.properties p ON pr.property_id = p.property_id
        WHERE pr.recommendation_date >= CURRENT_DATE - INTERVAL '7 days'
            AND ABS(pr.price_change_pct) >= 3
        ORDER BY ABS(pr.price_change_pct) DESC LIMIT 50
    """)

    if not recs_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Recommendations", f"{len(recs_df)}")
        col2.metric("Avg Price Change", f"{recs_df['price_change_pct'].mean():.1f}%")
        col3.metric("Avg Confidence", f"{recs_df['confidence_score'].mean():.2f}")

        st.dataframe(recs_df, use_container_width=True)

    st.subheader("Price Distribution by City")
    price_df = run_query("""
        SELECT location_city as city, base_price
        FROM silver.properties WHERE is_active = TRUE
    """)

    if not price_df.empty:
        fig = px.box(price_df, x='city', y='base_price',
                     title=None, labels={'base_price': 'Base Price ($)'})
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Price vs Rating")
        pr_df = run_query("""
            SELECT base_price, property_rating, property_type
            FROM silver.properties
            WHERE is_active = TRUE AND property_rating IS NOT NULL
        """)
        if not pr_df.empty:
            fig = px.scatter(pr_df, x='base_price', y='property_rating',
                             color='property_type', opacity=0.6,
                             labels={'base_price': 'Price ($)', 'property_rating': 'Rating'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Price by Property Type")
        avg_price_df = run_query("""
            SELECT property_type, ROUND(AVG(base_price)::DECIMAL, 2) as avg_price,
                COUNT(*) as count
            FROM silver.properties WHERE is_active = TRUE
            GROUP BY property_type ORDER BY avg_price DESC
        """)
        if not avg_price_df.empty:
            fig = px.bar(avg_price_df, x='property_type', y='avg_price',
                         color='count', title=None)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**AirStay Analytics v1.0**")
st.sidebar.markdown("*FastAPI + Streamlit + PostGIS*")
st.sidebar.markdown("*Kafka + Spark + Airflow + dbt*")
