"""
Load Testing for AirStay Analytics API
Simulates realistic user behavior and traffic patterns
"""
import random
from datetime import datetime, timedelta
from locust import HttpUser, task, between, events
from locust.env import Environment
import json

# Test data
CITIES = ["New York", "Los Angeles", "San Francisco", "Chicago", "Miami"]
PROPERTY_IDS = list(range(1, 501))  # Assuming 500 properties
USER_IDS = list(range(1001, 2001))  # Assuming 1000 users


class PropertySearchUser(HttpUser):
    """Simulates users searching for properties"""
    
    wait_time = between(1, 5)  # Wait 1-5 seconds between tasks
    
    def on_start(self):
        """Initialize user session"""
        self.user_id = random.choice(USER_IDS)
        self.api_prefix = "/api/v1"
    
    @task(10)  # Weight 10 - most common task
    def search_properties(self):
        """Search for properties"""
        params = {
            'city': random.choice(CITIES),
            'num_guests': random.randint(1, 4),
            'page': 1,
            'page_size': 20
        }
        
        # Randomly add optional filters
        if random.random() > 0.5:
            params['min_bedrooms'] = random.randint(1, 3)
        
        if random.random() > 0.7:
            params['max_price'] = random.choice([150, 250, 500])
        
        with self.client.get(
            f"{self.api_prefix}/properties/search",
            params=params,
            name="/properties/search",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and len(data['data']) > 0:
                    # Simulate user clicking on a property
                    self.selected_property = random.choice(data['data'])['property_id']
                    response.success()
                else:
                    response.failure("No properties returned")
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(5)  # Weight 5
    def view_property_details(self):
        """View detailed property information"""
        property_id = random.choice(PROPERTY_IDS)
        
        with self.client.get(
            f"{self.api_prefix}/properties/{property_id}",
            name="/properties/:id",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                response.failure("Property not found")
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(3)  # Weight 3
    def check_availability(self):
        """Check property availability"""
        property_id = random.choice(PROPERTY_IDS)
        start_date = datetime.now() + timedelta(days=random.randint(7, 60))
        end_date = start_date + timedelta(days=random.randint(2, 7))
        
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }
        
        with self.client.get(
            f"{self.api_prefix}/properties/{property_id}/availability",
            params=params,
            name="/properties/:id/availability",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(2)  # Weight 2
    def get_recommendations(self):
        """Get personalized recommendations"""
        params = {
            'user_id': self.user_id,
            'n_recommendations': 10
        }
        
        # Sometimes filter by city
        if random.random() > 0.5:
            params['city'] = random.choice(CITIES)
        
        with self.client.get(
            f"{self.api_prefix}/recommendations/user/{self.user_id}",
            params=params,
            name="/recommendations/user/:id",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(1)  # Weight 1 - less common
    def create_booking(self):
        """Create a booking (POST request)"""
        property_id = random.choice(PROPERTY_IDS)
        check_in = datetime.now() + timedelta(days=random.randint(7, 60))
        check_out = check_in + timedelta(days=random.randint(2, 7))
        
        booking_data = {
            'property_id': property_id,
            'guest_id': self.user_id,
            'check_in_date': check_in.strftime('%Y-%m-%d'),
            'check_out_date': check_out.strftime('%Y-%m-%d'),
            'num_guests': random.randint(1, 4)
        }
        
        with self.client.post(
            f"{self.api_prefix}/bookings",
            json=booking_data,
            name="/bookings [POST]",
            catch_response=True
        ) as response:
            if response.status_code in [200, 201]:
                response.success()
            elif response.status_code == 409:
                # Property not available - expected
                response.success()
            elif response.status_code == 400:
                # Validation error - also acceptable in load test
                response.success()
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(2)
    def get_price_recommendation(self):
        """Get pricing recommendation"""
        property_id = random.choice(PROPERTY_IDS)
        
        with self.client.get(
            f"{self.api_prefix}/pricing/{property_id}/recommend",
            name="/pricing/:id/recommend",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")


class AnalyticsUser(HttpUser):
    """Simulates users viewing analytics dashboards"""
    
    wait_time = between(5, 15)  # Longer wait times for dashboard users
    
    def on_start(self):
        self.api_prefix = "/api/v1"
    
    @task(5)
    def view_dashboard_summary(self):
        """View dashboard summary"""
        params = {'days': random.choice([7, 30, 90])}
        
        with self.client.get(
            f"{self.api_prefix}/analytics/dashboard/summary",
            params=params,
            name="/analytics/dashboard/summary",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(3)
    def view_property_performance(self):
        """View property performance"""
        property_id = random.choice(PROPERTY_IDS)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        }
        
        with self.client.get(
            f"{self.api_prefix}/analytics/property/{property_id}/performance",
            params=params,
            name="/analytics/property/:id/performance",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")
    
    @task(2)
    def view_real_time_metrics(self):
        """View real-time metrics"""
        with self.client.get(
            f"{self.api_prefix}/analytics/metrics/real-time",
            name="/analytics/metrics/real-time",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}")


# Event hooks for custom metrics
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when test starts"""
    print("🚀 Load test starting...")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when test stops"""
    print("\n✅ Load test completed")
    
    stats = environment.stats
    
    print(f"\n📊 Performance Summary:")
    print(f"   Total requests: {stats.total.num_requests}")
    print(f"   Failures: {stats.total.num_failures}")
    print(f"   Avg response time: {stats.total.avg_response_time:.2f}ms")
    print(f"   Min response time: {stats.total.min_response_time:.2f}ms")
    print(f"   Max response time: {stats.total.max_response_time:.2f}ms")
    print(f"   RPS: {stats.total.total_rps:.2f}")
    
    # Performance thresholds
    if stats.total.avg_response_time > 500:
        print("\n⚠️  WARNING: Average response time exceeds 500ms")
    
    if stats.total.fail_ratio > 0.01:
        print(f"\n⚠️  WARNING: Failure rate {stats.total.fail_ratio*100:.2f}% exceeds 1%")