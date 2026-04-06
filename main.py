from datetime import date, datetime
from decimal import Decimal

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel, Field

app = FastAPI(title="Property Management API")

# CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "mgmt-545-project"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# -------------------------------------------------------------------
# Pydantic models
# -------------------------------------------------------------------
class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: str | None = None
    monthly_rent: float | None = Field(default=None, ge=0)


class PropertyUpdate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: str | None = None
    monthly_rent: float | None = Field(default=None, ge=0)


class IncomeCreate(BaseModel):
    amount: float = Field(ge=0)
    income_date: date
    source: str
    notes: str | None = None


class ExpenseCreate(BaseModel):
    amount: float = Field(ge=0)
    expense_date: date
    category: str
    notes: str | None = None


# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------
def serialize_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def serialize_row(row):
    return {key: serialize_value(value) for key, value in dict(row).items()}


def property_exists(property_id: int, bq: bigquery.Client) -> bool:
    query = f"""
        SELECT 1
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    results = list(bq.query(query, job_config=job_config).result())
    return len(results) > 0


def get_next_id(table_name: str, id_column: str, bq: bigquery.Client) -> int:
    query = f"""
        SELECT COALESCE(MAX({id_column}), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.{table_name}`
    """

    result = list(bq.query(query).result())
    return int(result[0]["next_id"])


def get_property_by_id_from_db(property_id: int, bq: bigquery.Client):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    results = list(bq.query(query, job_config=job_config).result())

    if not results:
        return None

    return serialize_row(results[0])


def get_income_by_id_from_db(income_id: int, bq: bigquery.Client):
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            income_date,
            source,
            notes
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE income_id = @income_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("income_id", "INT64", income_id)
        ]
    )

    results = list(bq.query(query, job_config=job_config).result())

    if not results:
        return None

    return serialize_row(results[0])


def get_expense_by_id_from_db(expense_id: int, bq: bigquery.Client):
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            expense_date,
            category,
            notes
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE expense_id = @expense_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expense_id", "INT64", expense_id)
        ]
    )

    results = list(bq.query(query, job_config=job_config).result())

    if not results:
        return None

    return serialize_row(results[0])

# -------------------------------------------------------------------
# Properties
# -------------------------------------------------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [serialize_row(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a single property by ID.
    """
    property_record = get_property_by_id_from_db(property_id, bq)

    if property_record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    return property_record


@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(payload: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new property.
    """
    new_property_id = get_next_id("properties", "property_id", bq)

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
        (
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        )
        VALUES
        (
            @property_id,
            @name,
            @address,
            @city,
            @state,
            @postal_code,
            @property_type,
            @tenant_name,
            @monthly_rent
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", new_property_id),
            bigquery.ScalarQueryParameter("name", "STRING", payload.name),
            bigquery.ScalarQueryParameter("address", "STRING", payload.address),
            bigquery.ScalarQueryParameter("city", "STRING", payload.city),
            bigquery.ScalarQueryParameter("state", "STRING", payload.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", payload.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", payload.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", payload.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", payload.monthly_rent),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create property: {str(e)}"
        )

    return {
        "message": "Property created successfully",
        "property": get_property_by_id_from_db(new_property_id, bq)
    }


@app.put("/properties/{property_id}")
def update_property(
    property_id: int,
    payload: PropertyUpdate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Updates an existing property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET
            name = @name,
            address = @address,
            city = @city,
            state = @state,
            postal_code = @postal_code,
            property_type = @property_type,
            tenant_name = @tenant_name,
            monthly_rent = @monthly_rent
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("name", "STRING", payload.name),
            bigquery.ScalarQueryParameter("address", "STRING", payload.address),
            bigquery.ScalarQueryParameter("city", "STRING", payload.city),
            bigquery.ScalarQueryParameter("state", "STRING", payload.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", payload.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", payload.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", payload.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", payload.monthly_rent),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update property: {str(e)}"
        )

    return {
        "message": "Property updated successfully",
        "property": get_property_by_id_from_db(property_id, bq)
    }


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Deletes a property and its related income/expense records.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    income_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    expenses_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
    """

    property_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        bq.query(income_query, job_config=job_config).result()
        bq.query(expenses_query, job_config=job_config).result()
        bq.query(property_query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete property: {str(e)}"
        )

    return {"message": f"Property {property_id} deleted successfully"}


@app.get("/properties/{property_id}/summary")
def property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns summary info for one property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    query = f"""
        SELECT
            p.property_id,
            p.name,
            p.address,
            p.tenant_name,
            p.monthly_rent,
            COALESCE((
                SELECT SUM(i.amount)
                FROM `{PROJECT_ID}.{DATASET}.income` i
                WHERE i.property_id = p.property_id
            ), 0) AS total_income,
            COALESCE((
                SELECT COUNT(*)
                FROM `{PROJECT_ID}.{DATASET}.income` i
                WHERE i.property_id = p.property_id
            ), 0) AS income_record_count,
            COALESCE((
                SELECT SUM(e.amount)
                FROM `{PROJECT_ID}.{DATASET}.expenses` e
                WHERE e.property_id = p.property_id
            ), 0) AS total_expenses,
            COALESCE((
                SELECT COUNT(*)
                FROM `{PROJECT_ID}.{DATASET}.expenses` e
                WHERE e.property_id = p.property_id
            ), 0) AS expense_record_count,
            COALESCE((
                SELECT SUM(i.amount)
                FROM `{PROJECT_ID}.{DATASET}.income` i
                WHERE i.property_id = p.property_id
            ), 0)
            -
            COALESCE((
                SELECT SUM(e.amount)
                FROM `{PROJECT_ID}.{DATASET}.expenses` e
                WHERE e.property_id = p.property_id
            ), 0) AS net_income
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        WHERE p.property_id = @property_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get property summary: {str(e)}"
        )

    return serialize_row(results[0])