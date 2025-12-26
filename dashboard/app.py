"""
AirStay Analytics Dashboard
Interactive analytics dashboard for hosts and administrators
"""
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
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
API_BASE_URL = "http://api:8000/api/v1"
DB_CONFIG = {
    'host': 'postgres',
    'database': 'airstay_db',
    'user': 'airstay',
    'password': 'airstay_pass'
}


# Database connection
@st.cache_resource
def get_db_connection():
    """Get cached database connection"""
    return psycopg2.connect(**DB_CONFIG)


# API helpers
def api_get(endpoint: str, params: dict = None):
    """Make GET request to API"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"API Error: {e}")
        return None


# Data fetchers
@st.cache_data(ttl=300)
def get_dashboard_summary(days: int = 30):
    """Get dashboard summary from API"""
    return api_get("/analytics/dashboard/summary", {"days": days})


@st.cache_data(ttl=60)
def get_real_time_metrics():
    """Get real-time metrics"""
    return api_get("/analytics/metrics/real-time")


@st.cache_data(ttl=300)
def get_property_performance(property_id: int, start_date, end_date):
    """Get property performance data"""
    return api_get(
        f"/analytics/property/{property_id}/performance",
        {"start_date": start_date, "end_date": end_date}
    )


@st.cache_data(ttl=300)
def get_city_metrics(city: str, start_date, end_date):
    """Get city metrics"""
    return api_get(
        f"/analytics/city/{city}/metrics",
        {"start_date": start_date, "end_date": end_date}
    )


@st.cache_data(ttl=600)
def get_properties_list():
    """Get list of properties"""
    conn = get_db_connection()
    query = """
        SELECT property_id, listing_id, title, location_city
        FROM silver.properties
        WHERE is_active = TRUE
        ORDER BY property_id
        LIMIT 100
    """
    df = pd.read_sql_query(query, conn)
    return df


@st.cache_data(ttl=600)
def get_cities_list():
    """Get list of cities"""
    conn = get_db_connection()
    query = """
        SELECT DISTINCT location_city as city
        FROM silver.properties
        WHERE is_active = TRUE
        ORDER BY city
    """
    df = pd.read_sql_query(query, conn)
    return df['city'].tolist()


# Sidebar
st.sidebar.title("🏠 AirStay Analytics")
st.sidebar.markdown("---")

view_mode = st.sidebar.radio(
    "Select View",
    ["Platform Overview", "Property Performance", "City Analysis", "Real-Time Metrics"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

# Date range selector
date_range = st.sidebar.selectbox(
    "Time Period",
    ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"]
)

if date_range == "Custom":
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("Start", datetime.now() - timedelta(days=30))
    end_date = col2.date_input("End", datetime.now())
else:
    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 90 days": 90
    }
    days = days_map[date_range]
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)


# Main content
if view_mode == "Platform Overview":
    st.title("📊 Platform Overview")
    st.markdown(f"*Data from {start_date} to {end_date}*")
    
    # Get summary data
    summary_data = get_dashboard_summary(days=(end_date - start_date).days)
    
    if summary_data:
        summary = summary_data['summary']
        
        # KPI Cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Bookings",
                f"{summary['total_bookings']:,}",
                f"{summary['growth_rate']:+.1f}%"
            )
        
        with col2:
            st.metric(
                "Total Revenue",
                f"${summary['total_revenue']:,.0f}",
                f"${summary['avg_booking_value']:.0f} avg"
            )
        
        with col3:
            st.metric(
                "Active Properties",
                f"{summary['active_properties']:,}",
                None
            )
        
        with col4:
            st.metric(
                "Unique Guests",
                f"{summary['unique_guests']:,}",
                f"{summary['avg_nights']:.1f} avg nights"
            )
        
        st.markdown("---")
        
        # Top Cities
        st.subheader("🌆 Top Performing Cities")
        
        if summary_data['top_cities']:
            cities_df = pd.DataFrame(summary_data['top_cities'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Bookings chart
                fig_bookings = px.bar(
                    cities_df,
                    x='city',
                    y='bookings',
                    title='Bookings by City',
                    color='bookings',
                    color_continuous_scale='Blues'
                )
                fig_bookings.update_layout(showlegend=False)
                st.plotly_chart(fig_bookings, use_container_width=True)
            
            with col2:
                # Revenue chart
                fig_revenue = px.bar(
                    cities_df,
                    x='city',
                    y='revenue',
                    title='Revenue by City',
                    color='revenue',
                    color_continuous_scale='Greens'
                )
                fig_revenue.update_layout(showlegend=False)
                st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Booking trends (mock data for demo)
        st.subheader("📈 Booking Trends")
        
        # Query actual booking trends
        conn = get_db_connection()
        trends_query = f"""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as bookings,
                SUM(total_price) as revenue
            FROM silver.bookings
            WHERE 
                created_at >= '{start_date}'
                AND created_at <= '{end_date}'
                AND booking_status IN ('confirmed', 'completed')
            GROUP BY DATE(created_at)
            ORDER BY date
        """
        trends_df = pd.read_sql_query(trends_query, conn)
        
        if not trends_df.empty:
            fig_trends = go.Figure()
            
            fig_trends.add_trace(go.Scatter(
                x=trends_df['date'],
                y=trends_df['bookings'],
                name='Bookings',
                mode='lines+markers',
                line=dict(color='#1f77b4', width=2)
            ))
            
            fig_trends.add_trace(go.Scatter(
                x=trends_df['date'],
                y=trends_df['revenue'],
                name='Revenue ($)',
                mode='lines+markers',
                yaxis='y2',
                line=dict(color='#2ca02c', width=2)
            ))
            
            fig_trends.update_layout(
                title='Daily Bookings and Revenue',
                xaxis_title='Date',
                yaxis_title='Bookings',
                yaxis2=dict(
                    title='Revenue ($)',
                    overlaying='y',
                    side='right'
                ),
                hovermode='x unified',
                height=400
            )
            
            st.plotly_chart(fig_trends, use_container_width=True)


elif view_mode == "Property Performance":
    st.title("🏡 Property Performance")
    
    # Property selector
    properties_df = get_properties_list()
    
    selected_property = st.selectbox(
        "Select Property",
        options=properties_df['property_id'].tolist(),
        format_func=lambda x: f"{properties_df[properties_df['property_id']==x]['listing_id'].values[0]} - {properties_df[properties_df['property_id']==x]['title'].values[0]}"
    )
    
    if selected_property:
        # Get performance data
        performance_data = get_property_performance(
            selected_property,
            start_date,
            end_date
        )
        
        if performance_data and len(performance_data) > 0:
            perf_df = pd.DataFrame(performance_data)
            perf_df['metric_date'] = pd.to_datetime(perf_df['metric_date'])
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Bookings",
                    f"{perf_df['bookings_count'].sum():,}"
                )
            
            with col2:
                st.metric(
                    "Total Revenue",
                    f"${perf_df['revenue_total'].sum():,.0f}"
                )
            
            with col3:
                st.metric(
                    "Avg Occupancy",
                    f"{perf_df['occupancy_rate'].mean():.1f}%"
                )
            
            with col4:
                st.metric(
                    "Avg Rating",
                    f"{perf_df['avg_rating'].mean():.2f} ⭐"
                )
            
            st.markdown("---")
            
            # Performance charts
            col1, col2 = st.columns(2)
            
            with col1:
                # Revenue over time
                fig_revenue = px.line(
                    perf_df,
                    x='metric_date',
                    y='revenue_total',
                    title='Daily Revenue',
                    markers=True
                )
                fig_revenue.update_traces(line_color='#2ca02c')
                st.plotly_chart(fig_revenue, use_container_width=True)
            
            with col2:
                # Occupancy over time
                fig_occupancy = px.line(
                    perf_df,
                    x='metric_date',
                    y='occupancy_rate',
                    title='Occupancy Rate (%)',
                    markers=True
                )
                fig_occupancy.update_traces(line_color='#ff7f0e')
                st.plotly_chart(fig_occupancy, use_container_width=True)
            
            # Detailed metrics table
            st.subheader("📋 Detailed Metrics")
            st.dataframe(
                perf_df[['metric_date', 'bookings_count', 'revenue_total', 
                        'occupancy_rate', 'avg_nightly_rate', 'avg_rating']],
                use_container_width=True
            )
        else:
            st.info("No performance data available for selected period")


elif view_mode == "City Analysis":
    st.title("🌆 City Analysis")
    
    # City selector
    cities = get_cities_list()
    selected_city = st.selectbox("Select City", cities)
    
    if selected_city:
        # Get city metrics
        city_data = get_city_metrics(selected_city, start_date, end_date)
        
        if city_data and len(city_data) > 0:
            city_df = pd.DataFrame(city_data)
            city_df['metric_date'] = pd.to_datetime(city_df['metric_date'])
            
            # Summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Active Properties",
                    f"{city_df['active_properties'].iloc[-1]:,}"
                )
            
            with col2:
                st.metric(
                    "Total Bookings",
                    f"{city_df['total_bookings'].sum():,}"
                )
            
            with col3:
                st.metric(
                    "Total Revenue",
                    f"${city_df['total_revenue'].sum():,.0f}"
                )
            
            st.markdown("---")
            
            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                fig_bookings = px.area(
                    city_df,
                    x='metric_date',
                    y='total_bookings',
                    title=f'Daily Bookings in {selected_city}'
                )
                st.plotly_chart(fig_bookings, use_container_width=True)
            
            with col2:
                fig_avg_price = px.line(
                    city_df,
                    x='metric_date',
                    y='avg_nightly_rate',
                    title='Average Nightly Rate',
                    markers=True
                )
                st.plotly_chart(fig_avg_price, use_container_width=True)


elif view_mode == "Real-Time Metrics":
    st.title("⚡ Real-Time Metrics")
    
    # Auto-refresh
    if st.button("🔄 Refresh"):
        st.rerun()
    
    # Get real-time data
    rt_data = get_real_time_metrics()
    
    if rt_data:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Bookings (Last Hour)",
                f"{rt_data['bookings_last_hour']:,}",
                None
            )
        
        with col2:
            st.metric(
                "Confirmed (Last Hour)",
                f"{rt_data['confirmed_last_hour']:,}",
                None
            )
        
        with col3:
            st.metric(
                "Revenue (Last Hour)",
                f"${rt_data['revenue_last_hour']:,.0f}",
                None
            )
        
        with col4:
            st.metric(
                "Active Users",
                f"{rt_data['active_users_last_hour']:,}",
                None
            )
        
        st.markdown("---")
        
        # Recent bookings
        st.subheader("📋 Recent Bookings")
        
        conn = get_db_connection()
        recent_query = """
            SELECT 
                b.booking_id,
                b.property_id,
                p.title,
                b.check_in_date,
                b.total_price,
                b.booking_status,
                b.created_at
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE b.created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            ORDER BY b.created_at DESC
            LIMIT 20
        """
        recent_df = pd.read_sql_query(recent_query, conn)
        
        if not recent_df.empty:
            st.dataframe(recent_df, use_container_width=True)
        else:
            st.info("No recent bookings in the last hour")


# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**AirStay Analytics v1.0**")
st.sidebar.markdown("*Powered by FastAPI + Streamlit*")