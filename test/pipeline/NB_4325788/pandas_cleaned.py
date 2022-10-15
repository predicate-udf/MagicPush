import pandas as pd
import pickle

books = pd.read_csv('BX-Books.csv', sep = ';', error_bad_lines=False, encoding='latin-1')
books.columns = ['ISBN', 'bookTitle', 'bookAuthor', 'yearOfPublication', 'publisher', 'imageUrlS', 'imageUrlM', 'imageUrlL']
users = pd.read_csv('BX-Users.csv', sep=';', error_bad_lines=False, encoding="latin-1")
users.columns = ['userID', 'Location', 'Age']
print(users.shape)
print(users[users['Location'].str.contains("usa|canada")].shape)
ratings = pd.read_csv('BX-Book-Ratings.csv', sep=';', error_bad_lines=False, encoding="latin-1")
ratings.columns = ['userID', 'ISBN', 'bookRating']

counts1 = ratings['userID'].value_counts()

ratings = ratings[ratings['userID'].isin(counts1[counts1 >= 200].index)]

counts = ratings['bookRating'].value_counts()

ratings = ratings[ratings['bookRating'].isin(counts[counts >= 100].index)]
combine_book_rating = pd.merge(ratings, books, on='ISBN')
columns = ['yearOfPublication', 'publisher', 'bookAuthor', 'imageUrlS', 'imageUrlM', 'imageUrlL']
combine_book_rating = combine_book_rating.drop(columns, axis=1)
combine_book_rating = combine_book_rating.dropna(axis = 0, subset = ['bookTitle'])

book_ratingCount = (combine_book_rating.
                    groupby(by = ['bookTitle'])['bookRating'].
                    count().
                    reset_index().
                    rename(columns= {'bookRating': 'totalRatingCount'})
                   [['bookTitle', 'totalRatingCount']]
                   )

rating_with_totalRatingCount = combine_book_rating.merge(book_ratingCount, 
                                                         left_on= 'bookTitle', 
                                                         right_on= 'bookTitle', 
                                                         how= 'left')


rating_popular_book = rating_with_totalRatingCount.query('totalRatingCount >= 50')
combined = rating_popular_book.merge(users, left_on = 'userID', right_on = 'userID', how = 'left')
us_canada_user_rating = combined[combined['Location'].str.contains("usa|canada")]
