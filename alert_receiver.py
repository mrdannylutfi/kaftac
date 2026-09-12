from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys

class GrafanaWebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Extract the content size to read exact byte string layout
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        try:
            # Parse raw payload bytes directly to a dictionary mapping
            alert_payload = json.loads(post_data.decode('utf-8'))
            
            print("\n" + "="*80)
            print(f"🚨 ALERT DISPATCHED FROM GRAFANA (Status: {alert_payload.get('status', 'UNKNOWN').upper()})")
            print("="*80)
            
            # Extract structured context variables from the provisioning layout
            for alert in alert_payload.get('alerts', []):
                annotations = alert.get('annotations', {})
                labels = alert.get('labels', {})
                
                print(f" Summary:     {annotations.get('summary', 'No summary provided')}")
                print(f" Description: {annotations.get('description', 'No description provided')}")
                print(f" Metric Name: {labels.get('alertname', 'N/A')}")
                print(f" Timestamp:   {alert.get('startsAt', 'N/A')}")
                print(f" Trace Link:  {alert_payload.get('externalURL', 'N/A')}")
                print("-" * 80)
                
        except json.JSONDecodeError:
            print("[ERROR] Received payload body, but parsing failed (Invalid JSON format).", file=sys.stderr)
            
        # Dispatch standardized clean HTTP 200 payload acknowledgment back to Grafana
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status":"RECEIVED"}')

    def log_message(self, format, *args):
        return # Suppress standard server text outputs to keep alert formatting clean

def run_listener():
    server_address = ('', 8082)
    httpd = HTTPServer(server_address, GrafanaWebhookHandler)
    print("🛰️  Lightweight Webhook Listener fully active on port 8082. Awaiting alerts...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down endpoint receiver safely.")
        httpd.server_close()

if __name__ == '__main__':
    run_listener()
