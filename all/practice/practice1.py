# -*- coding: utf-8 -*-
"""
This is a temporary script file.
"""

import random

print("Hello World")
# m=int(input())
# n=int(input())
m = 1
n = 5
sum1 = (m+n) * (n - m + 1) / 2
print(str(sum1))
a = random.random()

while m != a:
    print('It should be colon end of the while or if line')
    m = a
    if m != a:
        print('If')
    elif m == a:
        print('elseif in python is elif')
    else:
        print('else is same with other programs')
else:
    print('Python can recongnize where is the end by indentation')
        
# function can be defined between command lines


def calc_add(var1, var2):
    res = var1 + var2
    return res


# function must be defined before using
b = calc_add(1, 2)

fid = open('test1.txt', 'w')  # 'read, write, append'
# fid.readline()
# fid.readlines()

# list data type --> [ ]
var_int = [1, 2, 3, 4, 5]
for int2 in var_int:
    fid.write(str(int2)+'\n')
    print(int2)
# print(str(var_int.pop())) --> pop() cuts last element from variable.

# zero is the first index number
txt2 = [str(var_int[0])+' abc'+'\n', str(var_int[1])+' abc'+'\n', str(var_int[2])+' abc'+'\n']
fid.writelines(txt2)  # one argument please
fid.close()

with open('test1.txt', 'a') as fid:
    txt3 = [str(var_int[3])+' abc'+'\n', str(var_int[4])+' abc'+'\n']
    fid.writelines(txt3)  # one argument please

# there is no array in the python, so we must use the list data type
var_int2 = list()
var_int2[1:6] = range(1, 6)  # range starts from 1, end before 5
print ('print (var_int2[0:3]), slicing and printing 0th, 1st, 2nd elements')
print (var_int2[0:3])   # slicing and printing 0th, 1st, 2nd elements
print ('print (var_int2[:-1]), slicing and printing elements before end index')
print (var_int2[:-1])  # slicing and printing elements before end index
# var_int2.clear  # clear variable
# del(var_int2)   # clear variable

dict_a = dict()
dict_a['temp'] = 'temperature'
dict_a['salt'] = 'salinity'

# tuple data type
tuple_a = () 
tuple_a = 'First', [1, 2, 3], (1, 2, 3)
# sets data type, it similars with unique in MATLAB. It doesn't allow overlap
sets_a = {'Apple', 'Google', 'Microsoft', 'Apple', 'Microsoft'}
print(sets_a)


# define class which has properties of grid_x, grid_y and name
class User3:
    grid_x = ''
    grid_y = ''
    name = ''

    def init(self, grid_x, grid_y, name):
        self.grid_x = grid_x
        self.grid_y = grid_y
        self.name = name

    def get_grid_x(self):
        return self.grid_x

    def get_grid_y(self):
        return self.grid_y

    def get_name(self):
        return self.grid_y


# make classes which are managed in the list variable
user1 = User3()
user1.init('980', '920', 'nwp_1_20')
users = list()
users.append(user1)
