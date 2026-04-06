from pydantic import BaseModel
from typing import List


class CategorySuggestion(BaseModel):
    category: str
    confidence: float


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


class M1Output(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    predicted_category: str
    confidence: float
    top_3_suggestions: List[CategorySuggestion]
