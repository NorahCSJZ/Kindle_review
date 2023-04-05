import pandas as pd
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import create_engine
from sqlalchemy import String, Text, Date, DateTime, Integer
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime

Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customer'
    id: Mapped[str] = mapped_column(String(30), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=True)

    def __repr__(self) -> str:
        return f'id:{self.id},name:{self.name}'


class Book(Base):
    __tablename__ = 'book'
    id: Mapped[str] = mapped_column(String(10), primary_key=True)

    def __repr__(self) -> str:
        return f'id:{self.id}'


class Review(Base):
    __tablename__ = 'review'
    customer_id: Mapped[str] = mapped_column(
        ForeignKey('customer.id'), primary_key=True)
    book_id: Mapped[str] = mapped_column(
        ForeignKey('book.id'), primary_key=True)
    review_text: Mapped[str] = mapped_column(Text, nullable=True)
    review_time: Mapped[str] = mapped_column(Date)
    helpful: Mapped[int] = mapped_column(Integer)
    not_helpful: Mapped[int] = mapped_column(Integer)
    summary: Mapped[str] = mapped_column(Text, nullable=True)
    unix_time_stamp: Mapped[datetime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f'customer_id:{self.customer_id}, book_id:{self.book_id}, text:{self.review_text}, date:{self.review_time}, timestamp:{self.unix_time_stamp}'


def get_customer(data: pd.DataFrame) -> list:
    customer_dict = dict()
    for index, row in data[['reviewerID', 'reviewerName']].iterrows():
        if row['reviewerID'] not in customer_dict:
            customer_dict[row['reviewerID']] = None if pd.isnull(
                row['reviewerName']) else row['reviewerName']
    customers = list()
    for customer_id, customer_name in customer_dict.items():
        customers.append(Customer(id=str(customer_id), name=customer_name))
    return customers


def get_book(data: pd.DataFrame) -> list:
    books = set()
    for book_id in data['asin']:
        books.add(book_id)

    return [Book(id=book_id) for book_id in books]


def get_review(data: pd.DataFrame) -> list:
    reviews = list()
    for index, row in data[['reviewerID', 'asin', 'reviewText', 'reviewTime', 'helpful', 'summary', 'unixReviewTime']].iterrows():
        customer_id = row['reviewerID']
        book_id = row['asin']

        # "helpful" is used as string format [2,2] to remove the [and], split it with a comma, and convert it to an integer
        helpful = str(row['helpful']).removeprefix('[').removesuffix(']')
        help_not = [int(i) for i in helpful.split(',')]

        # "review text" may be an empty nan in pandas; set it to None in Python
        review_text = row['reviewText']
        if pd.isnull(row['reviewText']):
            review_text = None

        summary = row['summary']
        if pd.isnull(row['summary']):
            summary = None

        review_time = datetime.strptime(row['reviewTime'], '%m %d, %Y').date()

        unix_timestamp = datetime.fromtimestamp(row['unixReviewTime'])

        review = Review(customer_id=customer_id,
                        book_id=book_id,
                        review_text=review_text,
                        helpful=help_not[0], not_helpful=help_not[1],
                        summary=summary,
                        review_time=review_time,
                        unix_time_stamp=unix_timestamp)

        reviews.append(review)
    return reviews


def main():
    username = 'root'
    password = '9t3wpAIYZ1ooSY5qX9GltbV1WQg'
    host = '116.204.123.206'
    port = 16606
    base_db = 'mysql'

    # create database
    database_url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{base_db}'
    engine = create_engine(database_url)
    my_db = 'AmazonBookReview'
    with engine.connect() as conn:
        query = f'CREATE DATABASE {my_db}'
        conn.execute(text(query))

    # create tables
    database_url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{my_db}'
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)

    data = pd.read_csv('./kindle_reviews.csv')

    customers = get_customer(data)
    print('get customer')
    books = get_book(data)
    print('get books')
    reviews = get_review(data)
    print('get reviews')

    # insert into database
    with Session(engine) as session:
        session.add_all(customers)
        session.add_all(books)
        session.add_all(reviews)
        session.commit()


if __name__ == '__main__':
    main()
