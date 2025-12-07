"""
Geospatial Query Utilities
Search properties by location, distance, neighborhoods
Uses PostGIS for efficient spatial queries
"""
import logging
from typing import List, Dict, Tuple, Optional
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GeospatialSearchEngine:
    """Search properties using geospatial queries"""
    
    def __init__(self, db_config: Dict = None):
        """Initialize with database connection"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
    
    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def search_nearby_properties(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 5.0,
        property_type: Optional[str] = None,
        min_bedrooms: Optional[int] = None,
        max_guests: Optional[int] = None,
        max_price: Optional[float] = None,
        limit: int = 20
    ) -> List[Dict]:
        """
        Search properties within radius of a location
        
        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius_km: Search radius in kilometers
            property_type: Filter by property type
            min_bedrooms: Minimum bedrooms
            max_guests: Minimum guest capacity
            max_price: Maximum nightly price
            limit: Max results
            
        Returns:
            List of properties with distance
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                property_id,
                listing_id,
                title,
                property_type,
                bedrooms,
                bathrooms,
                max_guests,
                base_price,
                location_city,
                location_address,
                latitude,
                longitude,
                amenities,
                property_rating,
                review_count,
                property_images[1] as primary_image,
                ST_Distance(
                    location_point,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                ) / 1000.0 as distance_km
            FROM silver.properties
            WHERE 
                is_active = TRUE
                AND ST_DWithin(
                    location_point,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s * 1000
                )
        """
        
        params = [longitude, latitude, longitude, latitude, radius_km]
        
        # Add optional filters
        if property_type:
            query += " AND property_type = %s"
            params.append(property_type)
        
        if min_bedrooms:
            query += " AND bedrooms >= %s"
            params.append(min_bedrooms)
        
        if max_guests:
            query += " AND max_guests >= %s"
            params.append(max_guests)
        
        if max_price:
            query += " AND base_price <= %s"
            params.append(max_price)
        
        # Order by distance
        query += " ORDER BY distance_km LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {len(results)} properties within {radius_km}km")
        
        return [dict(row) for row in results]
    
    def search_in_polygon(
        self,
        polygon_coords: List[Tuple[float, float]],
        filters: Dict = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Search properties within a polygon (e.g., neighborhood boundary)
        
        Args:
            polygon_coords: List of (lat, lon) tuples defining polygon
            filters: Additional filters (property_type, bedrooms, etc.)
            limit: Max results
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Convert polygon coordinates to WKT format
        # Note: PostGIS expects (lon, lat) not (lat, lon)
        coords_str = ', '.join([f"{lon} {lat}" for lat, lon in polygon_coords])
        polygon_wkt = f"POLYGON(({coords_str}))"
        
        query = """
            SELECT 
                property_id,
                listing_id,
                title,
                property_type,
                bedrooms,
                base_price,
                location_city,
                neighborhood,
                latitude,
                longitude,
                property_rating
            FROM silver.properties
            WHERE 
                is_active = TRUE
                AND ST_Within(
                    location_point,
                    ST_GeomFromText(%s, 4326)
                )
        """
        
        params = [polygon_wkt]
        
        # Apply filters
        if filters:
            if filters.get('property_type'):
                query += " AND property_type = %s"
                params.append(filters['property_type'])
            
            if filters.get('min_bedrooms'):
                query += " AND bedrooms >= %s"
                params.append(filters['min_bedrooms'])
            
            if filters.get('max_price'):
                query += " AND base_price <= %s"
                params.append(filters['max_price'])
        
        query += " ORDER BY property_rating DESC NULLS LAST LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {len(results)} properties in polygon")
        
        return [dict(row) for row in results]
    
    def get_properties_between_points(
        self,
        point1: Tuple[float, float],
        point2: Tuple[float, float],
        max_distance_from_line_km: float = 2.0,
        limit: int = 30
    ) -> List[Dict]:
        """
        Find properties along a route between two points
        Useful for "properties between airport and downtown"
        
        Args:
            point1: (lat, lon) of start point
            point2: (lat, lon) of end point
            max_distance_from_line_km: Max distance from line
            limit: Max results
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        lat1, lon1 = point1
        lat2, lon2 = point2
        
        query = """
            SELECT 
                property_id,
                listing_id,
                title,
                property_type,
                base_price,
                location_city,
                latitude,
                longitude,
                ST_Distance(
                    location_point,
                    ST_MakeLine(
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    )
                ) / 1000.0 as distance_from_route_km
            FROM silver.properties
            WHERE 
                is_active = TRUE
                AND ST_DWithin(
                    location_point,
                    ST_MakeLine(
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ),
                    %s * 1000
                )
            ORDER BY distance_from_route_km
            LIMIT %s
        """
        
        params = [
            lon1, lat1, lon2, lat2,  # For distance calculation
            lon1, lat1, lon2, lat2,  # For DWithin
            max_distance_from_line_km,
            limit
        ]
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return [dict(row) for row in results]
    
    def cluster_properties_by_neighborhood(
        self,
        city: str,
        grid_size_km: float = 1.0
    ) -> List[Dict]:
        """
        Cluster properties into neighborhoods using spatial grid
        
        Args:
            city: City name
            grid_size_km: Grid cell size in km
            
        Returns:
            List of clusters with property counts and avg price
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Convert grid size to degrees (approximate)
        grid_degrees = grid_size_km / 111.0  # 1 degree ≈ 111 km
        
        query = """
            WITH grid_cells AS (
                SELECT 
                    FLOOR(latitude / %s) * %s as grid_lat,
                    FLOOR(longitude / %s) * %s as grid_lon,
                    COUNT(*) as property_count,
                    AVG(base_price) as avg_price,
                    AVG(property_rating) as avg_rating,
                    ARRAY_AGG(property_id) as property_ids
                FROM silver.properties
                WHERE 
                    location_city = %s
                    AND is_active = TRUE
                GROUP BY grid_lat, grid_lon
                HAVING COUNT(*) >= 3
            )
            SELECT 
                grid_lat,
                grid_lon,
                property_count,
                ROUND(avg_price::numeric, 2) as avg_price,
                ROUND(avg_rating::numeric, 2) as avg_rating,
                property_ids
            FROM grid_cells
            ORDER BY property_count DESC
        """
        
        params = [
            grid_degrees, grid_degrees,
            grid_degrees, grid_degrees,
            city
        ]
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {len(results)} neighborhood clusters in {city}")
        
        return [dict(row) for row in results]
    
    def calculate_density_heatmap(
        self,
        city: str,
        grid_size: int = 20
    ) -> List[Dict]:
        """
        Calculate property density for heatmap visualization
        
        Args:
            city: City name
            grid_size: Number of grid cells (grid_size x grid_size)
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            WITH city_bounds AS (
                SELECT 
                    MIN(latitude) as min_lat,
                    MAX(latitude) as max_lat,
                    MIN(longitude) as min_lon,
                    MAX(longitude) as max_lon
                FROM silver.properties
                WHERE location_city = %s
            ),
            grid AS (
                SELECT 
                    FLOOR((p.latitude - cb.min_lat) / (cb.max_lat - cb.min_lat) * %s)::int as grid_y,
                    FLOOR((p.longitude - cb.min_lon) / (cb.max_lon - cb.min_lon) * %s)::int as grid_x,
                    COUNT(*) as density,
                    AVG(p.base_price) as avg_price
                FROM silver.properties p
                CROSS JOIN city_bounds cb
                WHERE 
                    p.location_city = %s
                    AND p.is_active = TRUE
                GROUP BY grid_y, grid_x
            )
            SELECT 
                grid_x,
                grid_y,
                density,
                ROUND(avg_price::numeric, 2) as avg_price
            FROM grid
            ORDER BY density DESC
        """
        
        params = [city, grid_size, grid_size, city]
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return [dict(row) for row in results]


# Example usage
if __name__ == "__main__":
    engine = GeospatialSearchEngine()
    
    # Example 1: Search near specific location (Times Square, NYC)
    print("\n📍 Searching properties near Times Square...")
    nearby = engine.search_nearby_properties(
        latitude=40.7580,
        longitude=-73.9855,
        radius_km=2.0,
        min_bedrooms=2,
        limit=5
    )
    
    for prop in nearby:
        print(f"  - {prop['title']}: {prop['distance_km']:.2f}km away, ${prop['base_price']}/night")
    
    # Example 2: Cluster properties by neighborhood
    print("\n🗺️ Clustering properties in New York...")
    clusters = engine.cluster_properties_by_neighborhood(
        city='New York',
        grid_size_km=1.0
    )
    
    for cluster in clusters[:5]:
        print(f"  - Cluster at ({cluster['grid_lat']:.4f}, {cluster['grid_lon']:.4f}): "
              f"{cluster['property_count']} properties, avg ${cluster['avg_price']}/night")