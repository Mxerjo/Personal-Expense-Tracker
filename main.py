import pymysql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# MySQL connection details
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "expensetracker_db"

# Database connection
def get_db_connection():
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn

# Initialize the database
def initialize_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS expenses (
        id INT AUTO_INCREMENT PRIMARY KEY,
        description TEXT NOT NULL,
        amount FLOAT NOT NULL,
        category VARCHAR(255) NOT NULL,
        user_id INT,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        category_name VARCHAR(255) NOT NULL,
        balance FLOAT DEFAULT 0,
        user_id INT,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.commit()
    cursor.close()
    conn.close()

@app.on_event("startup")
def on_startup():
    initialize_db()

# Pydantic models
class User(BaseModel):
    username: str
    password: str

class Expense(BaseModel):
    description: str
    amount: float
    category: str

class CategoryCreate(BaseModel):
    category_name: str

class CategoryUpdate(BaseModel):
    amount: float

# User-related Functions

# 1. Create a new user
@app.post("/users/")
def create_user(user: User):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (username, password) VALUES (%s, %s)", (user.username, user.password))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "User created successfully"}

# 2. Retrieve a user by username
@app.get("/users/{username}")
def get_user(username: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

# 3. User login
@app.post("/login/")
def login(user: User):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s", (user.username, user.password))
    user_in_db = cursor.fetchone()
    cursor.close()
    conn.close()
    if user_in_db is None:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    return {"message": "Login successful", "user_id": user_in_db['id']}

# Category-related Functions

# 4. Create a new category
@app.post("/categories/")
def create_category(category: CategoryCreate, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO categories (category_name, user_id) VALUES (%s, %s)", (category.category_name, user_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Category created successfully"}

# 5. Get all categories for a user
@app.get("/categories/")
def get_categories(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM categories WHERE user_id = %s", (user_id,))
    categories = cursor.fetchall()
    cursor.close()
    conn.close()
    return categories

# 6. Get specific category by ID
@app.get("/categories/{category_id}")
def get_category(category_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM categories WHERE id = %s AND user_id = %s", (category_id, user_id))
    category = cursor.fetchone()
    cursor.close()
    conn.close()
    if category is None:
        raise HTTPException(status_code=404, detail="Category not found")
    return category

# 7. Add money to a category
@app.put("/categories/{category_id}/add-money/")
def add_money_to_category(category_id: int, update: CategoryUpdate, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM categories WHERE id = %s AND user_id = %s", (category_id, user_id))
    category = cursor.fetchone()
    if category is None:
        raise HTTPException(status_code=404, detail="Category not found")
    
    new_balance = category["balance"] + update.amount
    cursor.execute("UPDATE categories SET balance = %s WHERE id = %s", (new_balance, category_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": f"Added {update.amount} to category. New balance: {new_balance}"}

# 8. Subtract money from a category
@app.put("/categories/{category_id}/subtract-money/")
def subtract_money_from_category(category_id: int, update: CategoryUpdate, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM categories WHERE id = %s AND user_id = %s", (category_id, user_id))
    category = cursor.fetchone()
    if category is None:
        raise HTTPException(status_code=404, detail="Category not found")
    
    if category["balance"] < update.amount:
        raise HTTPException(status_code=400, detail="Insufficient funds")
    
    new_balance = category["balance"] - update.amount
    cursor.execute("UPDATE categories SET balance = %s WHERE id = %s", (new_balance, category_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": f"Subtracted {update.amount} from category. New balance: {new_balance}"}

# 9. Transfer money between categories
@app.put("/categories/transfer/")
def transfer_money_between_categories(from_category_id: int, to_category_id: int, amount: float, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get balances
    cursor.execute("SELECT * FROM categories WHERE id = %s AND user_id = %s", (from_category_id, user_id))
    from_category = cursor.fetchone()
    cursor.execute("SELECT * FROM categories WHERE id = %s AND user_id = %s", (to_category_id, user_id))
    to_category = cursor.fetchone()
    
    if from_category is None or to_category is None:
        raise HTTPException(status_code=404, detail="One of the categories not found")
    
    if from_category["balance"] < amount:
        raise HTTPException(status_code=400, detail="Insufficient funds in the source category")
    
    # Update balances
    new_from_balance = from_category["balance"] - amount
    new_to_balance = to_category["balance"] + amount
    
    cursor.execute("UPDATE categories SET balance = %s WHERE id = %s", (new_from_balance, from_category_id))
    cursor.execute("UPDATE categories SET balance = %s WHERE id = %s", (new_to_balance, to_category_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": f"Transferred {amount} from category {from_category_id} to {to_category_id}. New balances: {new_from_balance} and {new_to_balance}"}

# Expense-related Functions

# 10. Create a new expense
@app.post("/expenses/")
def add_expense(expense: Expense, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO expenses (description, amount, category, user_id) VALUES (%s, %s, %s, %s)", 
                 (expense.description, expense.amount, expense.category, user_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense added successfully"}

# 11. Get all expenses for a user
@app.get("/expenses/")
def get_expenses(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM expenses WHERE user_id = %s", (user_id,))
    expenses = cursor.fetchall()
    cursor.close()
    conn.close()
    return expenses

# 12. Get specific expense by ID
@app.get("/expenses/{expense_id}")
def get_expense(expense_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM expenses WHERE id = %s AND user_id = %s", (expense_id, user_id))
    expense = cursor.fetchone()
    cursor.close()
    conn.close()
    if expense is None:
        raise HTTPException(status_code=404, detail="Expense not found")
    return expense

# 13. Delete an expense
@app.delete("/expenses/{expense_id}")
def delete_expense(expense_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM expenses WHERE id = %s AND user_id = %s", (expense_id, user_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense deleted successfully"}

# 14. Update an expense
@app.put("/expenses/{expense_id}")
def update_expense(expense_id: int, updated_expense: Expense, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE expenses SET description = %s, amount = %s, category = %s WHERE id = %s AND user_id = %s", 
                 (updated_expense.description, updated_expense.amount, updated_expense.category, expense_id, user_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense updated successfully"}

# 15. Get total balance of all categories for a user
@app.get("/categories/balance/")
def get_total_balance(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(balance) AS total_balance FROM categories WHERE user_id = %s", (user_id,))
    total_balance = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"total_balance": total_balance["total_balance"]}

# 16. Get total amount of all expenses for a user
@app.get("/expenses/total/")
def get_total_expenses(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(amount) AS total_amount FROM expenses WHERE user_id = %s", (user_id,))
    total_expenses = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"total_expenses": total_expenses["total_amount"]}

# 17. Get all categories with positive balances for a user
@app.get("/categories/positive/")
def get_categories_with_balance(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM categories WHERE user_id = %s AND balance > 0", (user_id,))
    categories = cursor.fetchall()
    cursor.close()
    conn.close()
    return categories

# 18. Search expenses by description
@app.get("/expenses/search/{query}")
def search_expenses(query: str, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM expenses WHERE description LIKE %s AND user_id = %s", (f'%{query}%', user_id))
    expenses = cursor.fetchall()
    cursor.close()
    conn.close()
    return expenses

# 19. Get recent expenses for a user (limit 5)
@app.get("/expenses/recent/")
def get_recent_expenses(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM expenses WHERE user_id = %s ORDER BY date DESC LIMIT 5", (user_id,))
    recent_expenses = cursor.fetchall()
    cursor.close()
    conn.close()
    return recent_expenses

# 20. Get the expense summary by category for a user
@app.get("/expenses/summary/")
def get_expense_summary(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT category, SUM(amount) AS total_spent FROM expenses WHERE user_id = %s GROUP BY category", (user_id,))
    summary = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"category": row["category"], "total_spent": row["total_spent"]} for row in summary]
