"""
Demand Forecasting Model
Predicts future booking demand using time series analysis
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
import pickle

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DemandForecastingModel:
    """Forecast booking demand for properties"""
    
    def __init__(self, db_config: Dict = None):
        """Initialize forecasting model"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = []
        
        logger.info("✅ Demand Forecasting Model initialized")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def extract_historical_demand(
        self,
        property_id: int = None,
        city: str = None,
        lookback_days: int = 365
    ) -> pd.DataFrame:
        """
        Extract historical booking demand
        
        Args:
            property_id: Specific property (None for all)
            city: Specific city (None for all)
            lookback_days: Days of history
            
        Returns:
            DataFrame with daily demand data
        """
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                p.property_id,
                p.location_city,
                DATE(b.check_in_date) as booking_date,
                COUNT(*) as bookings_count,
                SUM(b.nights) as total_nights,
                AVG(b.total_price) as avg_price,
                SUM(b.total_price) as total_revenue
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE 
                b.booking_status IN ('confirmed', 'completed')
                AND b.check_in_date >= CURRENT_DATE - INTERVAL '%s days'
        """
        
        params = [lookback_days]
        
        if property_id:
            query += " AND p.property_id = %s"
            params.append(property_id)
        
        if city:
            query += " AND p.location_city = %s"
            params.append(city)
        
        query += " GROUP BY p.property_id, p.location_city, DATE(b.check_in_date)"
        query += " ORDER BY booking_date"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        logger.info(f"Extracted {len(df)} demand records")
        
        return df
    
    def create_time_series_features(
        self,
        df: pd.DataFrame,
        date_column: str = 'booking_date'
    ) -> pd.DataFrame:
        """
        Create time series features from dates
        
        Args:
            df: DataFrame with date column
            date_column: Name of date column
            
        Returns:
            DataFrame with added features
        """
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Temporal features
        df['day_of_week'] = df[date_column].dt.dayofweek
        df['day_of_month'] = df[date_column].dt.day
        df['week_of_year'] = df[date_column].dt.isocalendar().week
        df['month'] = df[date_column].dt.month
        df['quarter'] = df[date_column].dt.quarter
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_month_start'] = df[date_column].dt.is_month_start.astype(int)
        df['is_month_end'] = df[date_column].dt.is_month_end.astype(int)
        
        # Holiday indicator (simplified)
        df['is_holiday'] = df.apply(
            lambda row: self._is_holiday(row[date_column]),
            axis=1
        )
        
        # Season
        df['season'] = df['month'].apply(self._get_season)
        
        # Lag features (previous demand)
        if 'bookings_count' in df.columns:
            df = df.sort_values(date_column)
            
            for lag in [1, 7, 14, 30]:
                df[f'bookings_lag_{lag}'] = df.groupby('property_id')['bookings_count'].shift(lag)
            
            # Rolling statistics
            for window in [7, 14, 30]:
                df[f'bookings_rolling_mean_{window}'] = (
                    df.groupby('property_id')['bookings_count']
                    .rolling(window=window, min_periods=1)
                    .mean()
                    .reset_index(0, drop=True)
                )
        
        # Fill NaN from lag features
        df = df.fillna(0)
        
        return df
    
    def _is_holiday(self, date: pd.Timestamp) -> int:
        """Check if date is a US holiday"""
        holidays = [
            (1, 1),   # New Year
            (7, 4),   # Independence Day
            (12, 25), # Christmas
            (11, 24), # Thanksgiving (approximate)
        ]
        return 1 if (date.month, date.day) in holidays else 0
    
    def _get_season(self, month: int) -> int:
        """Get season from month (encoded)"""
        if month in [12, 1, 2]:
            return 0  # winter
        elif month in [3, 4, 5]:
            return 1  # spring
        elif month in [6, 7, 8]:
            return 2  # summer
        else:
            return 3  # fall
    
    def prepare_training_data(
        self,
        df: pd.DataFrame,
        target_column: str = 'bookings_count',
        test_size: float = 0.2
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Prepare training and test sets
        
        Args:
            df: Full dataset
            target_column: Target variable
            test_size: Fraction for test set
            
        Returns:
            X_train, X_test, y_train, y_test
        """
        # Define features
        self.feature_names = [
            'day_of_week', 'day_of_month', 'week_of_year', 'month', 'quarter',
            'is_weekend', 'is_month_start', 'is_month_end', 'is_holiday', 'season',
            'bookings_lag_1', 'bookings_lag_7', 'bookings_lag_14', 'bookings_lag_30',
            'bookings_rolling_mean_7', 'bookings_rolling_mean_14', 'bookings_rolling_mean_30'
        ]
        
        # Filter available features
        self.feature_names = [f for f in self.feature_names if f in df.columns]
        
        # Split by time (last test_size of data)
        split_idx = int(len(df) * (1 - test_size))
        
        df_sorted = df.sort_values('booking_date')
        
        train_df = df_sorted.iloc[:split_idx]
        test_df = df_sorted.iloc[split_idx:]
        
        X_train = train_df[self.feature_names]
        y_train = train_df[target_column]
        
        X_test = test_df[self.feature_names]
        y_test = test_df[target_column]
        
        logger.info(f"Training set: {len(X_train)}, Test set: {len(X_test)}")
        
        return X_train, X_test, y_train, y_test
    
    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        model_type: str = 'gbm'
    ):
        """
        Train forecasting model
        
        Args:
            X_train: Training features
            y_train: Training targets
            model_type: 'rf' or 'gbm'
        """
        logger.info(f"🚀 Training {model_type} forecasting model...")
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        
        # Initialize model
        if model_type == 'rf':
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                random_state=42,
                n_jobs=-1
            )
        else:  # gbm
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                random_state=42
            )
        
        # Train
        self.model.fit(X_train_scaled, y_train)
        
        logger.info("✅ Model training completed")
    
    def evaluate(
        self,
        X_test: pd.DataFrame,
        y_test: pd.Series
    ) -> Dict:
        """Evaluate model performance"""
        X_test_scaled = self.scaler.transform(X_test)
        y_pred = self.model.predict(X_test_scaled)
        
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        metrics = {
            'mae': round(mae, 2),
            'rmse': round(rmse, 2),
            'mean_actual_demand': round(y_test.mean(), 2),
            'mean_predicted_demand': round(y_pred.mean(), 2)
        }
        
        logger.info(f"📈 Forecast Metrics: {metrics}")
        
        return metrics
    
    def forecast(
        self,
        property_id: int,
        forecast_days: int = 30
    ) -> pd.DataFrame:
        """
        Forecast demand for next N days
        
        Args:
            property_id: Property ID
            forecast_days: Days to forecast
            
        Returns:
            DataFrame with forecasted demand
        """
        # Get recent data for lag features
        recent_data = self.extract_historical_demand(
            property_id=property_id,
            lookback_days=60
        )
        
        if len(recent_data) == 0:
            logger.warning(f"No historical data for property {property_id}")
            return pd.DataFrame()
        
        # Create features
        recent_data = self.create_time_series_features(recent_data)
        
        # Generate future dates
        last_date = recent_data['booking_date'].max()
        future_dates = pd.date_range(
            start=last_date + timedelta(days=1),
            periods=forecast_days,
            freq='D'
        )
        
        # Create future feature dataframe
        future_df = pd.DataFrame({
            'property_id': property_id,
            'booking_date': future_dates
        })
        
        future_df = self.create_time_series_features(future_df)
        
        # Use most recent values for lag features (simplified)
        for col in self.feature_names:
            if col.startswith('bookings_lag') or col.startswith('bookings_rolling'):
                if col in recent_data.columns:
                    future_df[col] = recent_data[col].iloc[-1]
        
        # Predict
        X_future = future_df[self.feature_names]
        X_future_scaled = self.scaler.transform(X_future)
        
        predictions = self.model.predict(X_future_scaled)
        
        # Ensure non-negative
        predictions = np.maximum(predictions, 0)
        
        future_df['forecasted_demand'] = predictions
        
        return future_df[['booking_date', 'forecasted_demand']]
    
    def save_model(self, filepath: str):
        """Save model to disk"""
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_names': self.feature_names
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"✅ Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load model from disk"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_names = model_data['feature_names']
        
        logger.info(f"✅ Model loaded from {filepath}")


# Example usage
if __name__ == "__main__":
    model = DemandForecastingModel()
    
    # Extract historical data
    demand_data = model.extract_historical_demand(lookback_days=180)
    
    # Create features
    demand_data = model.create_time_series_features(demand_data)
    
    # Train
    X_train, X_test, y_train, y_test = model.prepare_training_data(demand_data)
    model.train(X_train, y_train, model_type='gbm')
    
    # Evaluate
    metrics = model.evaluate(X_test, y_test)
    
    # Forecast
    forecast = model.forecast(property_id=1, forecast_days=30)
    print(f"\n📈 30-day Forecast:\n{forecast.head(10)}")