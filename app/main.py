from fastapi import FastAPI
import psycopg2
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

# Setup tracing
resource = Resource(attributes={"service.name": "petite-bank"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="signoz-otel-collector.monitoring.svc.cluster.local:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

app = FastAPI()

# Instrument FastAPI
FastAPIInstrumentor.instrument_app(app)

def get_db():
    return psycopg2.connect(
        host="postgres",
        database="petitebank",
        user="admin",
        password="password123"
    )

@app.on_event("startup")
def startup():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(50) PRIMARY KEY,
            balance INTEGER DEFAULT 100
        )
    """)
    cur.execute("INSERT INTO users (user_id, balance) VALUES ('user1', 100) ON CONFLICT DO NOTHING")
    cur.execute("INSERT INTO users (user_id, balance) VALUES ('user2', 100) ON CONFLICT DO NOTHING")
    cur.execute("INSERT INTO users (user_id, balance) VALUES ('user3', 100) ON CONFLICT DO NOTHING")
    conn.commit()
    cur.close()
    conn.close()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/balance/{user}")
def balance(user: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE user_id = %s", (user,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {"user": user, "balance": row[0]}
    return {"user": user, "balance": 0}

@app.post("/transfer/{sender}/{receiver}/{amount}")
def transfer(sender: str, receiver: str, amount: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE user_id = %s", (sender,))
    row = cur.fetchone()
    if row and row[0] >= amount:
        cur.execute("UPDATE users SET balance = balance - %s WHERE user_id = %s", (amount, sender))
        cur.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (amount, receiver))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "ok"}
    cur.close()
    conn.close()
    return {"status": "error"}
