package vgame_db

import java.beans.Statement
import java.sql.{Connection, DriverManager, SQLException}
import scala.io.StdIn
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._

object VideoGamesDBP1 {

  def main(args: Array[String]): Unit = {

    var valid_cred = false
    var user = "";
    var pass = "";
    var privileges ="";
    var mainmenuselection = ""
    var mainmenucheck = false
    var optionmenuselection = ""
    var optionmenucheck = false
    var programexitcheck = false

    //CONNECT TO DATABASE TO GET USERS LOGIN INFO//
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = System.getenv("JDBC_URL")
    val username = System.getenv("JDBC_USER")
    val password = System.getenv("JDBC_PASSWORD")

    var connection: Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()


    //INITIATE SPARK SESSION//
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    //PROGRAM LOOP
    do {

      valid_cred = false
      user = "";
      pass = "";
      mainmenuselection = ""
      mainmenucheck = false
      optionmenuselection = ""
      optionmenucheck = false
      programexitcheck = false

      //MAIN MENU//
      do {
        println("Welcome to the main menu, please make a selection")
        println("1) Sign In")
        println("2) Exit")
        print("> ")
        mainmenuselection = StdIn.readLine()
        println()

        mainmenuselection match {
          case "1" =>
            //USER SIGN IN//
            do {
              print("Enter your username: ")
              user = StdIn.readLine()
              print("Enter your password: ")
              pass = StdIn.readLine()
              println()

              var resultSet = statement.executeQuery("SELECT * FROM users;")
              resultSet.next()

              breakable {
                do {
                  var check_user = resultSet.getString(2)
                  var check_pass = resultSet.getString(3)
                  privileges = resultSet.getString(4)

                  if (check_user == user && check_pass == pass) {
                    valid_cred = true
                    mainmenucheck = true
                    println("Success! Welcome '" + user + "' with '" + privileges + "' privileges")
                    println()
                    break
                  }

                } while (resultSet.next())

                if (valid_cred == false) {
                  println(Console.RED + "ERROR: INCORRECT USER OR PASSWORD, TRY AGAIN" + Console.RESET)
                  println()
                }

              }

            } while (!valid_cred)

          case "2" =>
            mainmenucheck = true
            programexitcheck = true

          case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)

        }

      } while (!mainmenucheck)


      if (valid_cred) {

        //OPTION MENU
        do {
          println("What would you like to do today, " + user + "?")
          println("1) Solve the problems")
          if(privileges == "admin"){
            println("2) Add user -ADMIN ONLY OPTION-")
            println("3) Delete user -ADMIN ONLY OPTION")
          }
          println("4) Back")
          print("> ")
          optionmenuselection = StdIn.readLine()
          println()

          if((optionmenuselection == "2" || optionmenuselection == "3") && privileges != "admin"){
            println(Console.YELLOW + "WARNING: ADMIN PRIVILEGE REQUIRED TO ACCESS THIS FEATURES" + Console.RESET)
            println()
            optionmenuselection = "thisdoesnothing"
          }

          optionmenuselection match {
            case "1" =>
              ///VIDEO GAME SALES DATA FROM DEC 2016, SOLVE QUERY PROBLEMS ONE BY ONE
              spark.sql("DROP table IF EXISTS vgamedata")
              spark.sql("create table IF NOT EXISTS vgamedata(Name varchar(255), Platform varchar(255), Year_of_Release varchar(255), " +
                "Genre String, Publisher varchar(255), NA_Sales Double, EU_Sales Double, JP_Sales Double, Other_Sales Double, " +
                "Global_Sales Double) row format delimited fields terminated by ','");
              spark.sql("LOAD DATA LOCAL INPATH 'input/videogames.csv' INTO TABLE vgamedata")
//              spark.sql("SELECT Count(*) AS TOTALCOUNT FROM vgamedata").show()
//              spark.sql("SELECT Count(*) AS NumBranch2BevAFile FROM BevA WHERE BevA.BranchID='Branch2'").show()
//              spark.sql("SELECT * FROM vgamedata").show()

              //PROBLEM 1
              println("What CONSOLE has the most VIDEO GAME sales globally?")
              spark.sql("SELECT DISTINCT Platform, SUM(Global_Sales) OVER (PARTITION BY Platform) AS Total_Global_Sales FROM vgamedata ORDER BY Total_Global_Sales DESC").show(3000, false)
              println("Enter something to continue")
              StdIn.readLine()

              //PROBLEM 2
              println("What game genre is the most popular globally?")
              spark.sql("SELECT DISTINCT Genre, SUM(Global_Sales) OVER (PARTITION BY Genre) AS Total_Global_Sales FROM vgamedata ORDER BY Total_Global_Sales DESC").show(3000,false)
              println("Enter something to continue")
              StdIn.readLine()

              //PROBLEM 3
              println("What game genre is the most popular in Japan?")
              spark.sql("SELECT DISTINCT Genre, SUM(JP_Sales) OVER (PARTITION BY Genre) AS Total_JP_Sales FROM vgamedata ORDER BY Total_JP_Sales DESC").show(3000,false)
              println("Enter something to continue")
              StdIn.readLine()

              //PROBLEM 4
              println("What game genre is the most popular in North America?")
              spark.sql("SELECT DISTINCT Genre, SUM(NA_Sales) OVER (PARTITION BY Genre) AS Total_NA_Sales FROM vgamedata ORDER BY Total_NA_Sales DESC").show(3000,false)
              println("Enter something to continue")
              StdIn.readLine()

              //PROBLEM 5
              println("What has been the year with more video game releases?")
              spark.sql("SELECT DISTINCT Year_of_Release, COUNT(Name) OVER(PARTITION BY Year_of_Release) AS Total_Games_Released FROM vgamedata ORDER BY Total_Games_Released DESC").show(3000,false)
              println("Enter something to continue")
              StdIn.readLine()

              //PROBLEM 6 FUTURE
              println("What publishers are expected to earn more revenue compared to their competitors in the next 20 years?")
              spark.sql("SELECT DISTINCT Publisher, SUM(Global_Sales) OVER(PARTITION BY Publisher) AS Sales_by_Publisher FROM vgamedata ORDER BY Sales_by_Publisher DESC").show(3000,false)
              println("Enter something to continue")
              StdIn.readLine()

            case "2" =>
              //ADD A USER
              print("What is the name of the new user?: ")
              var newuser = StdIn.readLine()
              print("What is the password for this user?: ")
              var newpass = StdIn.readLine()

              var newpriv = ""

              breakable {
                do {
                  println("Would like to give this user admin or basic privileges?: ")
                  println("1) admin")
                  println("2) basic")
                  print("> ")
                  newpriv = StdIn.readLine()

                  newpriv match {
                    case "1" =>
                      newpriv = "admin"
                      break;
                    case "2" =>
                      newpriv = "basic"
                      break;
                    case _ =>
                      println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
                      println()
                  }

                } while (true)

              }

              statement.executeUpdate("INSERT INTO users (user_name,user_password,user_privileges) \n" +
                "VALUES ('" + newuser + "','" + newpass + "','" + newpriv + "');")

              println(Console.BLUE + "SUCCESS! USER HAS BEEN ADDED!" + Console.RESET)
              println()

            case "3" =>
              //DELETE A USER
              print("What is the User ID of the user you are trying to delete? ")
              var userid = StdIn.readLine()
              println()

              var sql = "SELECT * FROM users WHERE user_id = " + userid + ";"
              var resultSet = statement.executeQuery(sql)

              if(resultSet.next() == false){
                println(Console.YELLOW + "THIS USER DOES NOT EXISTS" + Console.RESET)
                println()
              }
              else{
                statement.executeUpdate("DELETE FROM users WHERE user_id = " + userid + ";")
                println(Console.BLUE + "USER DELETED SUCCESSFULLY" + Console.RESET)
                println()
              }

            case "4" => optionmenucheck = true
            case "thisdoesnothing" =>
            case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
            println()
          }

        } while (!optionmenucheck)


      }

    } while(!programexitcheck)

    println("Thank you for using my app, Goodbye!")


  }

}
