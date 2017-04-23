from entity import *

class Section(Entity):
    _columns  = ['title']
    _parents  = []
    _children = {'categories': 'Category'}
    _siblings = {}

class Category(Entity):
    _columns  = ['title']
    _parents  = ['section']
    _children = {'posts': 'Post'}
    _siblings = {}

class Post(Entity):
    _columns  = ['content', 'title']
    _parents  = ['category']
    _children = {'comments': 'Comment'}
    _siblings = {'tags': 'Tag'}

class Comment(Entity):
    _columns  = ['text']
    _parents  = ['post', 'user']
    _children = {}
    _siblings = {}

class Tag(Entity):
    _columns  = ['name']
    _parents  = []
    _children = {}
    _siblings = {'posts': 'Post'}

class User(Entity):
    _columns  = ['name', 'email', 'age']
    _parents  = []
    _children = {'comments': 'Comment'}
    _siblings = {}


if __name__ == "__main__":
    for x in User.all():
        print(x.name)
    # post = Post(4)
    # for x in post.tags:
    #     print(x.name)

    # category = Category(4)
    # print(category.section.title)
    # for x in category.posts:
    #     print(x.content)