# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np

# %matplotlib inline   # for Jupyter Notebook

x = np.arange(360, step=20)
y = np.sin(x * 2 * np.pi / 360)

plt.plot(x, y)

plt.show()


# from datetime import datetime
#
# now = datetime.now()
# print now
#
# print now.year
# print now.month
# print now.day

# print '%02d/%02d/%04d' % (now.month, now.day, now.year)
# print '%02d-%02d-%04d' % (now.month, now.day, now.year)
# print '%02d/%02d/%04d %02d:%02d:%02d' % (now.year, now.month, now.day, now.hour, now.minute, now.second)




# """
# This program generates passages that are generated in mad-lib format
# Author: Katherin
# """
#
# # The template for the story
#
# STORY = "This morning %s woke up feeling %s. 'It is going to be a %s day!' Outside, a bunch of %s s were protesting to keep %s in stores. They began to %s to the rhythm of the %s, which made all the %s s very %s. Concerned, %s texted %s, who flew %s to %s and dropped %s in a puddle of frozen %s. %s woke up in the year %s, in a world where %s s ruled the world."
#
# print " Program has started"
#
# name = raw_input("Enter a name: ")
#
# adj1 = raw_input("Enter a adj1: ")
# adj2 = raw_input("Enter a adj2: ")
# adj3 = raw_input("Enter a adj3: ")
#
# verb1 = raw_input("Enter a verb1: ")
#
# noun1 = raw_input("Enter a noun1: ")
# noun2 = raw_input("Enter a noun2: ")
#
# animal = raw_input("Enter a animal: ")
# food = raw_input("Enter a food: ")
# fruit = raw_input("Enter a fruit: ")
# superhero = raw_input("Enter a superhero: ")
# country = raw_input("Enter a country: ")
# dessert = raw_input("Enter a dessert: ")
# year = raw_input("Enter a year: ")
#
# print STORY % (name, adj1, adj2, animal, food, verb1, noun1, fruit, adj3, name, superhero, name, country, name, dessert, name, year,noun2)

