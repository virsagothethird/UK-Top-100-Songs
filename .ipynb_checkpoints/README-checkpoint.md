# Analyzing the UK Top 100 Songs Chart

## Thesis

This project is aimed at determining if the month of a song's release affects it's lifespan on the top 100 chart.

The data was found on the website: https://www.officialcharts.com/. This included weekly charts from 2015 to 2019.


## What I found:

During the initial EDA, I found that there was a total of **2605 unique songs** with an overall average lifespan of **9.98 weeks**. 

Some filtering was needed as there was a significant number of songs that had a lifespan of only 1 week:

![hist1](https://github.com/virsagothethird/UK-Top-100-Songs/blob/master/img/Lifespan_counts_original.png)

I decided to create 2 thresholds: a peak rank of 75 and the year 2019. The first threshold made it so low lifespan songs didn't affect the data as heavily. The second allowed songs released later in the datast time to live out it's "life" on the chart.

The filter dataset included **1362 songs** with an average lifespan of **13.43 weeks**.

![bar1](https://github.com/virsagothethird/UK-Top-100-Songs/blob/master/img/num_songs_month.png)

Average number of songs released per month is highest in March with the two lowest being January and December. However, when we look at the next graph...

![bar2](https://github.com/virsagothethird/UK-Top-100-Songs/blob/master/img/avg_lifespan_month.png)

We see that songs released in January have the longest average lifespan and December has the lowest.


## Statistical Tests

1. Null Hypothesis: *The sample means are identical*
2. Alternative Hypothesis: *The sample means are not identical*
3. Statistical Test: *2 sample T-test*
4. Because a total of twelve T-tests will be performed, we will be using a Bonferroni corrected alpha of 0.004.


## Results

Running the twelve T-tests returned these p-values:

* corrected alpha = 0.004166666666666667

* Jan vs rest = 1.1238765120724023e-218
* Feb vs rest = 0.02221184602615466
* Mar vs rest = 0.021887849527000784
* Apr vs rest = 2.9852612307170152e-40
* May vs rest = 4.2279649417257105e-10
* Jun vs rest = 3.886858550605457e-06
* Jul vs rest = 0.0003782468859613491
* Aug vs rest = 8.702493589426346e-88
* Sep vs rest = 9.427033867124077e-10
* Oct vs rest = 0.0642181950040024
* Nov vs rest = 1.8142931441988762e-47
* Dec vs rest = 1.3248289999081888e-261

The resulting p-values show that besides February, March and October, we can confidently reject the null hypothesis. For those months, there is a significant enough statistical difference in average lifespans.


## Reflection and looking forward

I've heard that digging through data and doing EDA can be frustrating and tedious, however, while I do believe that is true to some extent, I found myself enjoying that portion of my project far more than I had originally thought. Going through the data, being able to explore it freely and see what appears was quite fascinating. Being able to visualize my findings was very satisfying to see.

Throughout this project, whether by looking at the data or consulting my peers, new questions began to emerge. This dataset can be further enriched by including data on genre, producer, song length, song tempo, etc. I think it would be quite interesting to build some sort of predictor that takes in all of these parameters to predict things like lifespan, peak rank, etc.


## Technologies Used
* AWS
* MongoDB
* Beautiful Soup
* Pyspark
* Matplotlib