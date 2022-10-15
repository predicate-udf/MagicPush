from email.headerregistry import Group
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

books = InitTable('BX-Books.pickle')
books1 = Rename(books, {'ISBN': 'ISBN', 'Book-Title': 'bookTitle', 'Book-Author': 'bookAuthor', 'Year-Of-Publication': 'yearOfPublication', 'Publisher': 'publisher', 'Image-URL-S': 'imageUrlS', 'Image-URL-M': 'imageUrlM', 'Image-URL-L': 'imageUrlL'})
users = InitTable('BX-Users.pickle')
users1 = Rename(users, {'User-ID': 'userID', 'Location': 'Location', 'Age': 'Age'})
ratings = InitTable('BX-Book-Ratings.pickle')
ratings1 = Rename(ratings, {'User-ID': 'userID', 'ISBN': 'ISBN', 'Book-Rating': 'bookRating'})

sub_op0 = SubpipeInput(ratings1, 'table')
sub_op_row = SubpipeInput(ratings1, 'row')
filter_row = ScalarComputation({'row':sub_op_row}, "lambda row: row['userID']")
sub_op1 = Filter(sub_op0, BinOp(Field('userID'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
ratings2 = CrosstableUDF(ratings1, 'user_count', SubPipeline(PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op2])), 'int')
ratings3 = Filter(ratings2, BinOp(Field('user_count'), ">=", Constant(200)))

sub_op0_1 = SubpipeInput(ratings3, 'table')
sub_op_row_1 = SubpipeInput(ratings3, 'row')
filter_row_1 = ScalarComputation({'row':sub_op_row_1}, "lambda row: row['bookRating']")
sub_op1_1 = Filter(sub_op0_1, BinOp(Field('bookRating'), '==', filter_row_1))
sub_op2_1 = AllAggregate(sub_op1_1, Value(0), 'lambda x,y: x+1')
ratings4 = CrosstableUDF(ratings3, 'rating_count', SubPipeline(PipelinePath([sub_op0_1, sub_op_row_1, filter_row_1, sub_op1_1, sub_op2_1])), 'int')
ratings5 = Filter(ratings4, BinOp(Field('rating_count'), ">=", Constant(100)))

ratings6 = DropColumns(ratings5, ['user_count', 'rating_count'])
combine_book_rating = InnerJoin(ratings1, books1, ['ISBN'], ['ISBN'])
combine_book_rating1 = DropColumns(combine_book_rating, ['yearOfPublication', 'publisher', 'bookAuthor', 'imageUrlS', 'imageUrlM', 'imageUrlL'])
combine_book_rating2 = DropNA(combine_book_rating1, ['bookTitle'])
book_ratingCount = GroupBy(combine_book_rating2, ['bookTitle'], {'bookRating':(Value(0),'count')}, {'bookRating':'totalRatingCount'})
rating_with_totalRatingCount = LeftOuterJoin(combine_book_rating2, book_ratingCount, ['bookTitle'], ['bookTitle'])
rating_popular_book = Filter(rating_with_totalRatingCount, BinOp(Field('totalRatingCount'), '>=', Constant(50)))
combined = LeftOuterJoin(rating_popular_book, users1, ['userID'],['userID'])
us_canada_user_rating = Filter(combined, BinOp(Constant('usa|canada'), 'subset', Field('Location')))
