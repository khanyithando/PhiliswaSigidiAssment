from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from database import get_db, Transaction
import logging

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Financial Transactions API",
    description="API for accessing transaction data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None
)

@app.get("/")
async def root():
    return {"message": "Welcome to the Financial Transactions API"}

@app.get("/transactions/{user_id}/summary")
async def get_transaction_summary(
    user_id: int,
    db: AsyncSession = Depends(get_db)
):
    try:
        result = await db.execute(
            select(
                func.count(Transaction.id),
                func.sum(Transaction.amount)
            ).where(Transaction.user_id == user_id)
        )
        count, total_amount = result.fetchone()

        if count == 0 or total_amount is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No transactions found for user {user_id}"
            )
        
        return {
            "user_id": user_id,
            "total_transactions": count,
            "total_amount": float(total_amount),
            "average_transaction_amount": float(total_amount) / count
        }
    except Exception as e:
        logger.error(f"Error fetching transactions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )