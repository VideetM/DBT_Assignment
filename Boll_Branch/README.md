# DBT Exercise Submission


### Project Organization
I have organized the project in the following way in the model folder:

- Staging- View of all the source table
- Intermediate Layer - Houses logic and transaformation
- Mart - Final Reports to be exposed to analyst and business users.


### Challenges Answers


- **Challenge 1- Data Modelling**\
    -**1.1** The **analyses folder** has file named ERD.png for **Entity relationship diagram.**\
    -**1.2** **Documentation of metrics** is availble in _sources.yml files in the **mart folder**.\Have utlized DBT metrics feature for the first time. It's a really cool addition. Still need some improvments in terms of features.
- **Challenge 2- Ad Hoc queries**
    - The **analyses folder** has file named **adhoc answers.sql** for **answered adhoc queries**.
- **Challenge 3 - Data Quality Review**\
    -**1** Option1 and Option 2 has product color and product size mismatch randomly. They don't follow any specific pattern. \I would investigate the   source data for this table. one suggestion would be to have defined for columns for color, size instead of option 1 or option 2.\
    **2** Title has entries of colors in it. \
    **3** Found cookie_id with invalid long string such  "\\" messing up the web event data. 

**Note:** I am available on email if you have questions regarding the exercise.

