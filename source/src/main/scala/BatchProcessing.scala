import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// -------------------------------------------------------------------------------------------------
//# Assumptions when computing balance and revenue
// -------------------------------------------------------------------------------------------------
//# 1 -> every deposit is a valid tx
//# 2 -> every bet which has a suffix of _B is bonus bet (no balance out of wallet)
//# 3 -> every bet which does not have a suffix of B is not a bonus bet (balance out of wallet)
//# 4 -> some bets may not be placed due insufficient funds (no balance)
//# 5 -> every win is a valid tx
//# 6 -> withdraw is valid if sufficient funds are available
//# 7 -> bonus bets do not consider a tax fee
// -------------------------------------------------------------------------------------------------

object BatchProcessing {

  // arguments
  // 1. input path
  // 2. output path
  // 3. master
  def main(args: Array[String]): Unit = {

    // get arguments from console
    val inputPath = args(0) // where the data resides at
    val outputPath = args(1) // where to output results
    val master = args(2) // master

    // setting spark configuration/properties which will be used by SparkContext
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("Balance/Revenue Batch Processing")
      .setMaster(master)

    // initialize spark context
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // load currency conversion values as Map[String, Double], will be used to convert to euro
    // values are based on the base currency EURO
    val currencyMap = csvToRdd(sparkContext, inputPath + "currency.csv")
      .map(currency => (currency(0), currency(1).toDouble))
      .collectAsMap()

    // load customers to RDD
    val timeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val customerRdd = csvToRdd(sparkContext, inputPath + "customers.csv")
      // transform to (customer_id, (customer_id, hashed_name, registration_date, country_code))
      .map(customer => (customer(0), ( // key/customer_id
        customer(0), // customer_id
        customer(1), // hashed_name
        timeFormat.parse(customer(2)).getTime, // registration_date
        customer(3) // country_code
      )))

    // load transactions to RDD
    val txsRdd = csvToRdd(sparkContext, inputPath + "transactions.csv")
      // transform to (transaction_id, customer_id, transaction_date,
      // (transaction_id, customer_id, transaction_date, currency, amount, tx_type, tx_suffix))
      .map(tx => ((tx(1), tx(0), timeFormat.parse(tx(2)).getTime), ( // key/transaction_id, customer_id, transaction_date
        tx(0), // transaction_id
        tx(1), // customer_id
        timeFormat.parse(tx(2)).getTime, // transaction_date
        tx(3), // currency
        tx(4).toDouble, // amount
        tx(5), // tx type
        tx(0).split("_")(1)(0)))) // tx_suffix
      // reduce by key to remove duplicated txs (keep latest/second)
      .reduceByKey((_, v2) => v2)
      // sort by key (includes timestamp)
      .sortByKey()
      // transform to (customer_id, (transaction_id, customer_id, transaction_date, currency, amount, tx_type, tx_suffix))
      .map(tx => (tx._1._1, tx._2))

    // -------------------------------------------------------------------------------------------------
    //# Assumptions when computing balance and revenue
    // -------------------------------------------------------------------------------------------------
    //# 1 -> every deposit is a valid tx
    //# 2 -> every bet which has a suffix of _B is bonus bet (no balance out of wallet)
    //# 3 -> every bet which does not have a suffix of B is not a bonus bet (balance out of wallet)
    //# 4 -> some bets may not be placed due insufficient funds (no balance)
    //# 5 -> every win is a valid tx
    //# 6 -> withdraw is valid if sufficient funds are available
    //# 7 -> bonus bets do not consider a tax fee
    // -------------------------------------------------------------------------------------------------

    // compute balance and P/L to the company for each customer
    val computeStats = (accumulator: (Double, Double), element: (String, String, Long, String, Double, String, Char)) => {

      // base_currency => 'EUR'
      // accumulator_1 => customer balance
      // accumulator_2 => P/L generated by customer's transactions (bet or win)

      // 'win' tx
      // -> accumulator_1 (increase customer balance)
      // -> accumulator_2 (convert to base currency and decrease from P/L)
      if (element._6 == "win") {

        // get base rate and convert to base currency
        val baseRate = currencyMap.get(element._4).get
        val convertedValue = if (element._5 == 0) 0 else element._5 / baseRate

        // (increase customer balance, decrease from P/L)
        (accumulator._1 + element._5, accumulator._2 - convertedValue)
      }

      // 'deposit' tx
      // -> accumulator_1 (increase customer balance)
      // -> accumulator_2 (no operation)
      else if (element._6 == "deposit") {

        // (increase customer balance, no operation)
        (accumulator._1 + element._5, accumulator._2)
      }

      // 'withdraw' tx
      // -> accumulator_1 (decrease customer balance if sufficient funds else no operation)
      // -> accumulator_2 (no operation)
      else if (element._6 == "withdraw") {

        // insufficient funds
        if (accumulator._1 < element._5) {

          // (no operation, no operation)
          (accumulator._1, accumulator._2)
        }

        // sufficient funds
        else {

          // (decrease customer balance, no operation)
          (accumulator._1 - element._5, accumulator._2)
        }
      }

      // 'bet' tx
      // -> accumulator_1 (decrease customer balance if tx is not bonus
      //                   else check sufficient funds if true reduce balance else invalid tx)
      // -> accumulator_2 (if transaction is valid, increase P/L)
      else if (element._6 == "bet") {

        // get base rate and convert to base currency
        val baseRate = currencyMap.get(element._4).get

        // compute 1% tax for a bet
        val tax = element._5 * 0.01
        val betRevenue = ((element._5 - tax) / baseRate)

        // bet is considered bonus since tx suffix contain 'B' / free bet
        if (element._7 == 'B') {

          // we have to increase P/L since we do not have a bet id to match a bet with a win
          // we assume that bets and wins will eventually negate each other when a win tx comes through

          // (no operation, increase P/L)
          (accumulator._1, accumulator._2 + betRevenue)
        }

        // not a fee bet must check if we need to subtract from balance
        else {

          // no sufficient funds (invalid transaction)
          if (accumulator._1 < element._5) {

            // (no operation, no operation)
            (accumulator._1, accumulator._2)
          }

          // sufficient funds
          else {

            // (decrease customer balance, increase to P/L)
            (accumulator._1 - element._5, accumulator._2 + betRevenue)
          }
        }
      }

      // else condition is needed even tho the conditions are exhaustive
      else {

        // (no operation, no operation)
        (accumulator._1, accumulator._2)
      }
    }

    // combine values
    val computationCombine = (v1: (Double, Double), v2: (Double, Double)) => (v1._1 + v2._1, v1._2 + v2._2)

    // computed result
    val computedResult = txsRdd.aggregateByKey((0.0, 0.0))(computeStats, computationCombine)

    // save customer balance since registration as csv file
    val balanceHeader: RDD[String] = sparkContext.parallelize(Array("Customerid,Balance,Calculation_date"))
    balanceHeader.union(computedResult
      // one string -> customer_id, balance, calculation datetime
      .map(balance => balance._1 + "," + f"${balance._2._1}%1.2f" + "," + java.time.LocalDateTime.now().toString))
      .repartition(1) // repartition to get all values in one node
      .saveAsTextFile(outputPath + "customers_balance.csv") // save as csv file

    // compute revenue for each country
    val revenuePerCountry = customerRdd
      .fullOuterJoin(computedResult) // outer join customer table with computed results
      .map(customer => ( // map to (Country, P/L from customer)
      customer._2._1.get._4, // country
      customer._2._2.getOrElse(0.0, 0.0)._2)) // P/L from customer
      .reduceByKey((x, y) => x + y) // reduce by key to get revenue per country

    // save revenue per country as csv file
    val revenueHeader: RDD[String] = sparkContext.parallelize(Array("Country,Net_profit_amount_eur,Calculation_date"))
    revenueHeader.union(revenuePerCountry
      // one string -> country, revenue, calculation datetime
      .map(country => country._1 + "," + f"${country._2}%1.2f" + "," + java.time.LocalDateTime.now().toString))
      .repartition(1) // repartition to get all values in/home/delinvas/repos/big-data-challenge/results/ one node
      .saveAsTextFile(outputPath + "net_country_profit.csv") // save as csv file

    // stop spark context
    sparkContext.stop()
  }

  // converts csv file to RDD
  def csvToRdd(context: SparkContext, path: String): RDD[Array[String]] = {

    // reads csv and converts to RDD of type string (each line)
    val rawCsvRdd: RDD[String] = context.textFile(path)

    // we need to drop the first row since it contains the header: drop index 0
    val csvRdd = rawCsvRdd.mapPartitionsWithIndex {
      (index, it) => if (index == 0) it.drop(1) else it
    }

    // split values by comma, Array[String]
    val rdd = csvRdd.map(_.split(","))

    // returns RDD
    return rdd
  }
}