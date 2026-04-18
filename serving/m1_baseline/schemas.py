from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict


class M1Input(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    date: str
    merchant: str
    amount: float
    transaction_type: str
    account_type: str
    day_of_week: int
    day_of_month: int
    month: int
    log_abs_amount: float
    historical_majority_category_for_payee: str


class CategorySuggestion(BaseModel):
    category: str
    confidence: float


class M1Output(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    predicted_category: str
    confidence: float
    top_3_suggestions: List[CategorySuggestion]


class M1FeedbackEntry(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    transaction_id: str
    date: str
    amount: float
    merchant: str
    imported_payee: Optional[str] = None
    predicted_category: str
    predicted_category_id: Optional[str] = None
    chosen_category: str
    chosen_category_id: Optional[str] = None
    confidence: float
    feedback_type: Literal["accepted", "overridden"]
    top_3_suggestions: List[CategorySuggestion] = []
    source: str = "actual"
    logged_at: Optional[str] = None
    model_name: Optional[str] = None
    model_version: Optional[str] = None


class M1FeedbackBatch(BaseModel):
    entries: List[M1FeedbackEntry]
