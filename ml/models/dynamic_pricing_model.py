"""
Dynamic Pricing Model with XGBoost
Predicts optimal nightly price based on property, market, and temporal features
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import pickle
import json

import xgboost as xgb
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns

from ml.features.pricing_features import PricingFeatureEngineer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DynamicPricingModel:
    """XGBoost model for dynamic pricing"""
    
    def __init__(self, feature_engineer: PricingFeatureEngineer = None):
        """Initialize pricing model"""
        self.feature_engineer = feature_engineer or PricingFeatureEngineer()
        self.model = None
        self.feature_names = None
        self.training_metrics = {}
        
        logger.info("✅ Dynamic Pricing Model initialized")
    
    def prepare_training_data(
        self,
        property_ids: List[int] = None,
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Prepare training and test datasets
        
        Args:
            property_ids: Optional list of property IDs
            test_size: Fraction of data for testing
            random_state: Random seed
            
        Returns:
            X_train, X_test, y_train, y_test
        """
        logger.info("📊 Preparing training data...")
        
        # Build feature matrix
        df = self.feature_engineer.build_feature_matrix(
            property_ids=property_ids,
            include_target=True
        )
        
        # Get feature names
        self.feature_names = self.feature_engineer.get_feature_names()
        
        # Ensure all feature columns exist
        missing_features = [f for f in self.feature_names if f not in df.columns]
        if missing_features:
            logger.warning(f"Missing features: {missing_features}")
            self.feature_names = [f for f in self.feature_names if f in df.columns]
        
        # Prepare X and y
        X = df[self.feature_names]
        y = df['base_price']
        
        # Remove outliers (prices > 99th percentile or < 1st percentile)
        lower_bound = y.quantile(0.01)
        upper_bound = y.quantile(0.99)
        
        mask = (y >= lower_bound) & (y <= upper_bound)
        X = X[mask]
        y = y[mask]
        
        logger.info(f"Dataset size after outlier removal: {len(X)}")
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=test_size,
            random_state=random_state
        )
        
        logger.info(f"Training set: {len(X_train)}, Test set: {len(X_test)}")
        
        return X_train, X_test, y_train, y_test
    
    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame = None,
        y_val: pd.Series = None,
        params: Dict = None
    ):
        """
        Train XGBoost model
        
        Args:
            X_train: Training features
            y_train: Training targets
            X_val: Validation features
            y_val: Validation targets
            params: XGBoost parameters
        """
        logger.info("🚀 Training XGBoost model...")
        
        # Default parameters
        default_params = {
            'objective': 'reg:squarederror',
            'max_depth': 6,
            'learning_rate': 0.1,
            'n_estimators': 200,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 3,
            'gamma': 0.1,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'n_jobs': -1,
        }
        
        if params:
            default_params.update(params)
        
        # Initialize model
        self.model = xgb.XGBRegressor(**default_params)
        
        # Prepare validation set
        eval_set = []
        if X_val is not None and y_val is not None:
            eval_set = [(X_train, y_train), (X_val, y_val)]
        
        # Train model
        self.model.fit(
            X_train,
            y_train,
            eval_set=eval_set if eval_set else None,
            early_stopping_rounds=20 if eval_set else None,
            verbose=10
        )
        
        logger.info("✅ Model training completed")
    
    def evaluate(
        self,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        save_plots: bool = True
    ) -> Dict:
        """
        Evaluate model performance
        
        Args:
            X_test: Test features
            y_test: Test targets
            save_plots: Whether to save evaluation plots
            
        Returns:
            Dictionary of metrics
        """
        logger.info("📊 Evaluating model...")
        
        # Predictions
        y_pred = self.model.predict(X_test)
        
        # Calculate metrics
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        
        # Mean Absolute Percentage Error
        mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
        
        metrics = {
            'mae': round(mae, 2),
            'rmse': round(rmse, 2),
            'r2': round(r2, 4),
            'mape': round(mape, 2)
        }
        
        self.training_metrics = metrics
        
        logger.info(f"📈 Model Metrics:")
        logger.info(f"   MAE: ${metrics['mae']}")
        logger.info(f"   RMSE: ${metrics['rmse']}")
        logger.info(f"   R²: {metrics['r2']}")
        logger.info(f"   MAPE: {metrics['mape']}%")
        
        if save_plots:
            self._plot_evaluation(y_test, y_pred)
        
        return metrics
    
    def _plot_evaluation(self, y_test: pd.Series, y_pred: np.ndarray):
        """Generate evaluation plots"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Actual vs Predicted
        axes[0, 0].scatter(y_test, y_pred, alpha=0.5)
        axes[0, 0].plot([y_test.min(), y_test.max()], 
                        [y_test.min(), y_test.max()], 
                        'r--', lw=2)
        axes[0, 0].set_xlabel('Actual Price')
        axes[0, 0].set_ylabel('Predicted Price')
        axes[0, 0].set_title('Actual vs Predicted Prices')
        
        # 2. Residuals
        residuals = y_test - y_pred
        axes[0, 1].scatter(y_pred, residuals, alpha=0.5)
        axes[0, 1].axhline(y=0, color='r', linestyle='--')
        axes[0, 1].set_xlabel('Predicted Price')
        axes[0, 1].set_ylabel('Residuals')
        axes[0, 1].set_title('Residual Plot')
        
        # 3. Residual distribution
        axes[1, 0].hist(residuals, bins=50, edgecolor='black')
        axes[1, 0].set_xlabel('Residuals')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].set_title('Residual Distribution')
        
        # 4. Feature importance
        importance = self.get_feature_importance(top_n=15)
        importance_df = pd.DataFrame(importance, columns=['Feature', 'Importance'])
        
        axes[1, 1].barh(importance_df['Feature'], importance_df['Importance'])
        axes[1, 1].set_xlabel('Importance')
        axes[1, 1].set_title('Top 15 Feature Importance')
        axes[1, 1].invert_yaxis()
        
        plt.tight_layout()
        plt.savefig('/tmp/pricing_model_evaluation.png', dpi=300, bbox_inches='tight')
        logger.info("✅ Evaluation plots saved to /tmp/pricing_model_evaluation.png")
        
        plt.close()
    
    def predict(
        self,
        property_id: int,
        target_date: datetime = None
    ) -> Dict:
        """
        Predict optimal price for a property
        
        Args:
            property_id: Property ID
            target_date: Date to predict for
            
        Returns:
            Dictionary with prediction and confidence interval
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        # Build features for single property
        df = self.feature_engineer.build_feature_matrix(
            property_ids=[property_id],
            target_date=target_date,
            include_target=False
        )
        
        if len(df) == 0:
            raise ValueError(f"No data found for property {property_id}")
        
        # Ensure feature order
        X = df[self.feature_names]
        
        # Predict
        predicted_price = self.model.predict(X)[0]
        
        # Get current price for comparison
        current_price = df.iloc[0].get('base_price', None)
        
        # Calculate confidence interval (simplified)
        # In production, use quantile regression or bootstrapping
        prediction_std = predicted_price * 0.1  # Assume 10% std
        confidence_interval = (
            predicted_price - 1.96 * prediction_std,
            predicted_price + 1.96 * prediction_std
        )
        
        result = {
            'property_id': property_id,
            'predicted_price': round(predicted_price, 2),
            'current_price': current_price,
            'price_change': round(predicted_price - current_price, 2) if current_price else None,
            'price_change_pct': round((predicted_price - current_price) / current_price * 100, 2) if current_price else None,
            'confidence_interval_lower': round(confidence_interval[0], 2),
            'confidence_interval_upper': round(confidence_interval[1], 2),
            'prediction_date': target_date.isoformat() if target_date else datetime.now().isoformat()
        }
        
        return result
    
    def batch_predict(
        self,
        property_ids: List[int],
        target_date: datetime = None
    ) -> pd.DataFrame:
        """
        Batch prediction for multiple properties
        
        Args:
            property_ids: List of property IDs
            target_date: Date to predict for
            
        Returns:
            DataFrame with predictions
        """
        logger.info(f"🔮 Predicting prices for {len(property_ids)} properties...")
        
        # Build features
        df = self.feature_engineer.build_feature_matrix(
            property_ids=property_ids,
            target_date=target_date,
            include_target=False
        )
        
        # Predict
        X = df[self.feature_names]
        predictions = self.model.predict(X)
        
        # Prepare results
        results_df = pd.DataFrame({
            'property_id': df['property_id'],
            'predicted_price': predictions,
            'current_price': df.get('base_price', None)
        })
        
        results_df['price_change'] = results_df['predicted_price'] - results_df['current_price']
        results_df['price_change_pct'] = (
            results_df['price_change'] / results_df['current_price'] * 100
        )
        
        logger.info(f"✅ Batch prediction completed")
        
        return results_df
    
    def get_feature_importance(self, top_n: int = 20) -> List[Tuple[str, float]]:
        """
        Get feature importance rankings
        
        Args:
            top_n: Number of top features to return
            
        Returns:
            List of (feature_name, importance) tuples
        """
        if self.model is None:
            raise ValueError("Model not trained")
        
        importance_dict = self.model.get_booster().get_score(importance_type='gain')
        
        # Map feature indices to names
        importance_list = [
            (self.feature_names[int(k.replace('f', ''))], v)
            for k, v in importance_dict.items()
        ]
        
        # Sort by importance
        importance_list.sort(key=lambda x: x[1], reverse=True)
        
        return importance_list[:top_n]
    
    def save_model(self, filepath: str):
        """Save model to disk"""
        model_data = {
            'model': self.model,
            'feature_names': self.feature_names,
            'feature_engineer_encoders': self.feature_engineer.encoders,
            'training_metrics': self.training_metrics,
            'trained_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"✅ Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load model from disk"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.feature_names = model_data['feature_names']
        self.feature_engineer.encoders = model_data['feature_engineer_encoders']
        self.training_metrics = model_data.get('training_metrics', {})
        
        logger.info(f"✅ Model loaded from {filepath}")
        logger.info(f"   Trained at: {model_data.get('trained_at')}")
        logger.info(f"   Metrics: {self.training_metrics}")


# Example usage
if __name__ == "__main__":
    # Initialize model
    model = DynamicPricingModel()
    
    # Prepare data
    X_train, X_test, y_train, y_test = model.prepare_training_data(
        test_size=0.2
    )
    
    # Train
    model.train(X_train, y_train, X_test, y_test)
    
    # Evaluate
    metrics = model.evaluate(X_test, y_test)
    
    # Save model
    model.save_model('/tmp/pricing_model.pkl')
    
    # Predict for a property
    prediction = model.predict(property_id=1)
    print(f"\n🔮 Price Prediction: {prediction}")
    
    # Feature importance
    importance = model.get_feature_importance(top_n=10)
    print(f"\n📊 Top 10 Features:")
    for feature, score in importance:
        print(f"   {feature}: {score:.2f}")