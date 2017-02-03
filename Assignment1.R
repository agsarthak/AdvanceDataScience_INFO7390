
# Read the csv
data <- read.csv("D:/GRAD_SCHOOL/Spring2017/ADS/Lab/ClassData.csv")

# look at top 5 rows of data
names(data)

######### 1. Min, Max, Median, Avg for GPA and YearofWorkExp ##########
summary(data$GPA)
summary(data$Years.of.work.experience)

######### 2. Find the mode of the Salary ######### 
getmode = function(x){ 
  ta = table(x)
  ta.max = max(ta)
  if (all(ta == ta.max))
    mod = NA
  else
    if(is.numeric(x))
      mod = as.numeric(names(ta)[ta == ta.max])
  else
    mod = names(ta)[ta == tam]
  return(mod)
}

getmode(data$Latest.salary..per.year.)


########## 3. % of students having Co/op and not having Co/op ##########

#count Y in the data after changing to upper case
with.coop.count <- count(data[toupper(data$Coops.Internships..YN.) == "Y",])
#count total records
total.count <- count(data)
#calculate percentage
100 * (with.coop.count/total.count)

#count N in the data after changing to upper case
without.coop.count <- count(data[toupper(data$Coops.Internships..YN.) == "N",])
#calculate percentage
100 * (without.coop.count/total.count)


######### 4. No of students with more than 500 LinkedIn contacts #########
count(data[data$Number.of.contacts.on.Linkedin > 500,])

######### 5. Find the Inter Quartile Range for the Expected Salary Range #########
summary(data$Latest.salary..per.year.)
IQR(data$Latest.salary..per.year.)
