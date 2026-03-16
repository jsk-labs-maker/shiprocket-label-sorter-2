"""
Shiprocket API Integration Module
Handles authentication, order management, shipping, and label downloads.
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import time


class ShiprocketAPI:
    """Shiprocket API client for order and shipping management."""
    
    BASE_URL = "https://apiv2.shiprocket.in/v1/external"
    
    def __init__(self, email: str = None, password: str = None):
        """Initialize with credentials from params or environment."""
        self.email = email or os.getenv("SHIPROCKET_EMAIL")
        self.password = password or os.getenv("SHIPROCKET_PASSWORD")
        self.token = None
        self.token_expiry = None
        
        if not self.email or not self.password:
            raise ValueError("Shiprocket credentials not provided. Set SHIPROCKET_EMAIL and SHIPROCKET_PASSWORD.")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with authentication token."""
        if not self.token or self._is_token_expired():
            self.authenticate()
        
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
    
    def _is_token_expired(self) -> bool:
        """Check if token is expired (with 1 hour buffer)."""
        if not self.token_expiry:
            return True
        return datetime.now() >= self.token_expiry - timedelta(hours=1)
    
    def authenticate(self) -> Dict[str, Any]:
        """Authenticate and get access token."""
        url = f"{self.BASE_URL}/auth/login"
        payload = {
            "email": self.email,
            "password": self.password
        }
        
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        
        self.token = data.get("token")
        # Token valid for 10 days, set expiry with buffer
        self.token_expiry = datetime.now() + timedelta(days=9)
        
        return data
    
    def get_orders(self, status: str = "NEW", per_page: int = 50, page: int = 1, days: int = 7) -> Dict[str, Any]:
        """
        Fetch orders by status, filtered to last N days.
        
        Args:
            status: Order status (NEW, READY_TO_SHIP, PICKUP_SCHEDULED, etc.)
            per_page: Number of orders per page (max 50)
            page: Page number
            days: Number of days to look back (default 7)
        
        Returns:
            Dict with orders data
        """
        url = f"{self.BASE_URL}/orders"
        
        # Calculate date range
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        params = {
            "filter": status.lower(),
            "per_page": per_page,
            "page": page,
            "from": from_date,
            "to": to_date
        }
        
        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json()
    
    def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific order."""
        url = f"{self.BASE_URL}/orders/show/{order_id}"
        
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()
    
    def get_shipment_details(self, shipment_id: int) -> Dict[str, Any]:
        """Get shipment details including AWB."""
        url = f"{self.BASE_URL}/shipments/{shipment_id}"
        
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()
    
    def assign_awb(self, shipment_id: int, courier_id: int = None) -> Dict[str, Any]:
        """
        Assign AWB to shipment (ship the order).
        
        Args:
            shipment_id: The shipment ID to assign AWB to
            courier_id: Optional courier ID. If not provided, uses priority settings.
        
        Returns:
            Dict with AWB assignment response
        """
        url = f"{self.BASE_URL}/courier/assign/awb"
        payload = {"shipment_id": shipment_id}
        
        if courier_id:
            payload["courier_id"] = courier_id
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    
    def bulk_ship_orders(self, shipment_ids: List[int], delay: float = 0.5) -> List[Dict[str, Any]]:
        """
        Ship multiple orders using auto courier assignment.
        
        Args:
            shipment_ids: List of shipment IDs to ship
            delay: Delay between API calls to avoid rate limiting
        
        Returns:
            List of results for each shipment
        """
        results = []
        
        for shipment_id in shipment_ids:
            try:
                result = self.assign_awb(shipment_id)
                result["shipment_id"] = shipment_id
                result["success"] = result.get("awb_assign_status") == 1
                results.append(result)
            except requests.exceptions.RequestException as e:
                results.append({
                    "shipment_id": shipment_id,
                    "success": False,
                    "error": str(e)
                })
            
            if delay > 0:
                time.sleep(delay)
        
        return results
    
    def get_available_couriers(self, 
                               pickup_postcode: str, 
                               delivery_postcode: str,
                               weight: float,
                               cod: bool = False,
                               order_id: int = None) -> Dict[str, Any]:
        """Get available courier partners for a shipment."""
        url = f"{self.BASE_URL}/courier/serviceability"
        params = {
            "pickup_postcode": pickup_postcode,
            "delivery_postcode": delivery_postcode,
            "weight": weight,
            "cod": 1 if cod else 0
        }
        
        if order_id:
            params["order_id"] = order_id
        
        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json()
    
    def generate_label(self, shipment_ids: List[int]) -> bytes:
        """
        Generate shipping labels for shipments.
        
        Args:
            shipment_ids: List of shipment IDs (max 50)
        
        Returns:
            PDF content as bytes
        """
        url = f"{self.BASE_URL}/courier/generate/label"
        payload = {"shipment_id": shipment_ids}
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        
        data = response.json()
        
        # The API returns a URL to the label PDF
        if "label_url" in data:
            label_response = requests.get(data["label_url"])
            label_response.raise_for_status()
            return label_response.content
        elif "label_created" in data and data.get("label_url"):
            label_response = requests.get(data["label_url"])
            label_response.raise_for_status()
            return label_response.content
        
        return data
    
    def get_label_url(self, shipment_ids: List[int]) -> str:
        """
        Get label PDF URL for shipments.
        
        Args:
            shipment_ids: List of shipment IDs
        
        Returns:
            URL to download label PDF
        """
        url = f"{self.BASE_URL}/courier/generate/label"
        payload = {"shipment_id": shipment_ids}
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        
        data = response.json()
        return data.get("label_url", "")
    
    def get_manifest(self, shipment_ids: List[int]) -> Dict[str, Any]:
        """Generate manifest for shipments."""
        url = f"{self.BASE_URL}/manifests/generate"
        payload = {"shipment_id": shipment_ids}
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    
    def request_pickup(self, shipment_ids: List[int], pickup_date: str = None) -> Dict[str, Any]:
        """
        Request pickup for shipped orders.
        
        Args:
            shipment_ids: List of shipment IDs
            pickup_date: Optional pickup date (YYYY-MM-DD). Defaults to tomorrow.
        
        Returns:
            Pickup request response
        """
        url = f"{self.BASE_URL}/courier/generate/pickup"
        
        if not pickup_date:
            pickup_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        payload = {
            "shipment_id": shipment_ids,
            "pickup_date": pickup_date
        }
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_tracking(self, awb: str = None, shipment_id: int = None, order_id: int = None) -> Dict[str, Any]:
        """
        Get tracking information for a shipment.
        
        Args:
            awb: AWB number
            shipment_id: Shipment ID
            order_id: Order ID
        
        Returns:
            Tracking information
        """
        if awb:
            url = f"{self.BASE_URL}/courier/track/awb/{awb}"
        elif shipment_id:
            url = f"{self.BASE_URL}/courier/track/shipment/{shipment_id}"
        elif order_id:
            url = f"{self.BASE_URL}/courier/track?order_id={order_id}"
        else:
            raise ValueError("Provide either awb, shipment_id, or order_id")
        
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()
    
    def cancel_shipment(self, awb_codes: List[str]) -> Dict[str, Any]:
        """Cancel shipments by AWB codes."""
        url = f"{self.BASE_URL}/orders/cancel/shipment/awbs"
        payload = {"awbs": awb_codes}
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_wallet_balance(self) -> Dict[str, Any]:
        """Get current wallet balance."""
        url = f"{self.BASE_URL}/account/details/wallet-balance"
        
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()


# Convenience functions for quick operations
def quick_ship_new_orders(email: str = None, password: str = None, limit: int = 50) -> Dict[str, Any]:
    """
    Quick function to ship all new orders with auto courier assignment.
    
    Returns summary of shipped orders.
    """
    api = ShiprocketAPI(email, password)
    
    # Get new orders
    orders_response = api.get_orders(status="NEW", per_page=limit)
    orders = orders_response.get("data", [])
    
    if not orders:
        return {"message": "No new orders to ship", "shipped": 0}
    
    # Extract shipment IDs
    shipment_ids = []
    for order in orders:
        if "shipments" in order and order["shipments"]:
            if isinstance(order["shipments"], list):
                for shipment in order["shipments"]:
                    shipment_ids.append(shipment["id"])
            elif isinstance(order["shipments"], dict):
                shipment_ids.append(order["shipments"]["id"])
    
    if not shipment_ids:
        return {"message": "No shipments found", "shipped": 0}
    
    # Ship all orders
    results = api.bulk_ship_orders(shipment_ids)
    
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    
    return {
        "total_orders": len(orders),
        "shipped": len(successful),
        "failed": len(failed),
        "results": results
    }


def download_labels_for_shipped_orders(email: str = None, password: str = None, 
                                        output_dir: str = "labels") -> Dict[str, Any]:
    """
    Download labels for all ready-to-ship orders.
    
    Returns dict with label file paths grouped by courier.
    """
    import os
    
    api = ShiprocketAPI(email, password)
    
    # Get ready to ship orders
    orders_response = api.get_orders(status="READY_TO_SHIP", per_page=50)
    orders = orders_response.get("data", [])
    
    if not orders:
        return {"message": "No ready-to-ship orders", "labels": []}
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Group by courier
    courier_shipments = {}
    for order in orders:
        shipments = order.get("shipments", {})
        if isinstance(shipments, dict):
            shipments = [shipments]
        
        for shipment in shipments:
            courier = shipment.get("courier", "Unknown")
            shipment_id = shipment.get("id")
            if shipment_id:
                if courier not in courier_shipments:
                    courier_shipments[courier] = []
                courier_shipments[courier].append(shipment_id)
    
    # Download labels by courier
    label_files = []
    for courier, shipment_ids in courier_shipments.items():
        try:
            label_url = api.get_label_url(shipment_ids)
            if label_url:
                response = requests.get(label_url)
                if response.status_code == 200:
                    filename = f"{datetime.now().strftime('%Y-%m-%d')}_{courier.replace(' ', '_')}_labels.pdf"
                    filepath = os.path.join(output_dir, filename)
                    with open(filepath, "wb") as f:
                        f.write(response.content)
                    label_files.append({
                        "courier": courier,
                        "count": len(shipment_ids),
                        "file": filepath
                    })
        except Exception as e:
            print(f"Error downloading labels for {courier}: {e}")
    
    return {
        "total_shipments": sum(len(ids) for ids in courier_shipments.values()),
        "couriers": list(courier_shipments.keys()),
        "labels": label_files
    }


if __name__ == "__main__":
    # Test the API
    from dotenv import load_dotenv
    load_dotenv()
    
    api = ShiprocketAPI()
    
    # Test authentication
    print("Authenticating...")
    auth = api.authenticate()
    print(f"Logged in as: {auth.get('email')}")
    
    # Get wallet balance
    print("\nWallet Balance:")
    balance = api.get_wallet_balance()
    print(f"  Balance: ₹{balance.get('data', {}).get('balance_amount', 'N/A')}")
    
    # Get new orders
    print("\nFetching new orders...")
    orders = api.get_orders(status="NEW")
    print(f"  Found {len(orders.get('data', []))} new orders")
