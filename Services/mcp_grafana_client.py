#!/usr/bin/env python3
# filepath: mcp_grafana_client.py

import argparse
import json
import os
import sys
import requests
from typing import List, Dict, Optional, Any
import logging


class Dashboard:
    def __init__(self, uid: str, title: str, url: str = None, json_data: str = None):
        self.uid = uid
        self.title = title
        self.url = url
        self.json = json_data


class DashboardCreateResponse:
    def __init__(self, uid: str, url: str, status: str = None, version: int = None, id: int = None):
        self.uid = uid
        self.url = url
        self.status = status
        self.version = version
        self.id = id


class MCPGrafanaClient:
    def __init__(self, server_url: str, api_key: str, timeout: int = 30, verbose: bool = False):
        self.server_url = server_url
        self.api_key = api_key
        self.timeout = timeout
        self.verbose = verbose
        
        # Configure logging
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(format='%(levelname)s: %(message)s', level=level)
        self.logger = logging.getLogger('mcp-grafana')
        
        if self.verbose:
            self.logger.debug(f"Initialized MCP Grafana client for {server_url}")
    
    def close(self):
        """Clean up any resources used by the client"""
        if self.verbose:
            self.logger.debug("Closed MCP Grafana client")
    
    def _get_headers(self):
        """Return headers for API requests"""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def get_dashboards(self) -> List[Dashboard]:
        """Retrieve all available dashboards"""
        url = f"{self.server_url}/api/search?type=dash-db"
        
        if self.verbose:
            self.logger.debug(f"GET {url}")
        
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout
            )
            response.raise_for_status()
            
            dashboards = []
            for item in response.json():
                dashboards.append(Dashboard(
                    uid=item.get("uid", ""),
                    title=item.get("title", ""),
                    url=item.get("url", "")
                ))
            
            return dashboards
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response:
                error_msg = f"Server error: {e.response.status_code} - {e.response.text}"
            else:
                error_msg = f"Request error: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def get_dashboard(self, uid: str) -> Dashboard:
        """Retrieve a specific dashboard by UID"""
        url = f"{self.server_url}/api/dashboards/uid/{uid}"
        
        if self.verbose:
            self.logger.debug(f"GET {url}")
        
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            meta = data.get("meta", {})
            dashboard_json = json.dumps(data.get("dashboard", {}))
            
            return Dashboard(
                uid=meta.get("uid", ""),
                title=meta.get("title", ""),
                json_data=dashboard_json
            )
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response:
                error_msg = f"Server error: {e.response.status_code} - {e.response.text}"
            else:
                error_msg = f"Request error: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def create_dashboard(self, json_content: str) -> DashboardCreateResponse:
        """Create a new dashboard from JSON content"""
        url = f"{self.server_url}/api/dashboards/db"
        
        if self.verbose:
            self.logger.debug(f"POST {url}")
        
        try:
            # Parse the JSON content
            dashboard_obj = json.loads(json_content)
            
            # Create the dashboard payload
            payload = {
                "dashboard": dashboard_obj,
                "overwrite": True
            }
            
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            return DashboardCreateResponse(
                uid=data.get("uid", ""),
                url=data.get("url", ""),
                status=data.get("status", ""),
                version=data.get("version", 0),
                id=data.get("id", 0)
            )
            
        except json.JSONDecodeError:
            error_msg = "Invalid dashboard JSON content"
            self.logger.error(error_msg)
            raise Exception(error_msg)
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response:
                error_msg = f"Server error: {e.response.status_code} - {e.response.text}"
            else:
                error_msg = f"Request error: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="MCP Grafana Client")
    
    # Define command line flags
    parser.add_argument("--server", default="http://localhost:3000", help="MCP Grafana server URL")
    parser.add_argument("--apikey", default="", help="API key for authentication")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Define commands
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--get-dashboards", action="store_true", help="Retrieve available dashboards")
    group.add_argument("--get-dashboard", help="Retrieve specific dashboard by UID")
    group.add_argument("--create-dashboard", help="Create dashboard from JSON file")
    
    args = parser.parse_args()
    
    # Check if API key is provided
    api_key = args.apikey
    if not api_key:
        # Try to get from environment variable
        api_key = os.environ.get("MCP_GRAFANA_API_KEY", "")
        if not api_key:
            print("API key is required. Provide it via --apikey flag or MCP_GRAFANA_API_KEY environment variable")
            print("\nUsage instructions:")
            parser.print_help()
            sys.exit(1)
    
    # Initialize the client
    client = MCPGrafanaClient(
        server_url=args.server,
        api_key=api_key,
        timeout=args.timeout,
        verbose=args.verbose
    )
    
    try:
        # Process commands
        if args.get_dashboards:
            dashboards = client.get_dashboards()
            for d in dashboards:
                print(f"Dashboard: {d.title} (UID: {d.uid})")
                
        elif args.get_dashboard:
            dashboard = client.get_dashboard(args.get_dashboard)
            print(f"Dashboard: {dashboard.title}")
            print(f"Content: {dashboard.json}")
            
        elif args.create_dashboard:
            try:
                with open(args.create_dashboard, 'r') as file:
                    json_content = file.read()
                
                result = client.create_dashboard(json_content)
                print(f"Dashboard created successfully! UID: {result.uid}, URL: {result.url}")
                
            except FileNotFoundError:
                print(f"Error: Dashboard file not found: {args.create_dashboard}")
                sys.exit(1)
                
            except Exception as e:
                print(f"Error creating dashboard: {str(e)}")
                sys.exit(1)
                
        else:
            print("No command specified. Use --help for usage information.")
            parser.print_help()
            
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
        
    finally:
        client.close()


if __name__ == "__main__":
    main()