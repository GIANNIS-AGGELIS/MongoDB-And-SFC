/***************************
 * Athor : Giannis Aggelis *
 *       11/90/2020         *
 ***************************/

using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using MongoDB.Bson.IO;
using System.Linq;
using System.Numerics;
using System.Threading;
using HilbertTransformation;
using System.Collections;
using System.Globalization;
using CsvHelper;
using System.IO;
using MathNet.Numerics.Distributions;
using MathNet.Numerics.Random;
using NUnit.Framework.Internal;
using System.Runtime.InteropServices.ComTypes;
using MathNet.Numerics;
using Tedd;



namespace MongoDBCurve
{
    class Program
    {
        static int NumThredsPreRecal = 4;
        static int NumThredsIndex = 3;
        static int NumThredsQueries = 6;
        static int[] SlideCount;
        static int SizeBlock;
        static List<GlobalUrbanPoint> AnserQuery;
        static List<GlobalUrbanPoint> CourdQuery;

        static void Main(string[] args)
        {
            MongoCRUD db = new MongoCRUD("Experiments");

            int bits = 8;
            int numExperemts = 1;
            int trueChance = 70;
            int lapPerSent = 100 - trueChance;
            string tableName = "Unif" + trueChance.ToString() + "Lapla" + lapPerSent.ToString() + "Res" + bits.ToString();

            /// courdinates for expiriments
            double lowX2d1PerSent = 10;
            double lowY2d1PerSent = 10;
            double higX2d1PerSent = 40;
            double higY2d1PerSent = 40;

            double lowX2d5PerSent = 10;
            double lowY2d5PerSent = 20;
            double higX2d5PerSent = 70;
            double higY2d5PerSent = 100;

            double lowX2d20PerSent = 150;
            double lowY2d20PerSent = 160;
            double higX2d20PerSent = 182;
            double higY2d20PerSent = 190;

            double lowX3d1PerSent = 13;
            double lowY3d1PerSent = 50;
            double lowZ3d1PerSent = 12;
            double higX3d1PerSent = 56;
            double higY3d1PerSent = 123;
            double higZ3d1PerSent = 87;

            double lowX3d5PerSent = 100;
            double lowY3d5PerSent = 100;
            double lowZ3d5PerSent = 100;
            double higX3d5PerSent = 230;
            double higY3d5PerSent = 176;
            double higZ3d5PerSent = 175;

            double lowX3d20PerSent = 170;
            double lowY3d20PerSent = 170;
            double lowZ3d20PerSent = 170;
            double higX3d20PerSent = 210;
            double higY3d20PerSent = 210;
            double higZ3d20PerSent = 210;



            /*       
            ///Create synthetic data
            List<GlobalUrbanPoint> syntheticData = CreateSunfeticDataNonUniform(1000000, trueChance, bits); // ~900mb of ram
  
            //// Add data to csv file
            var writer = new StreamWriter("D:\\Desktop\\CSV_Data\\"+tableName+".csv");
            var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);


            csv.WriteRecords<GlobalUrbanPoint>(syntheticData);
            writer.Flush();
            */



            /*
            /// Read The Csv File and pass to the databace
            var reader = new StreamReader("D:\\Desktop\\CSV_Data\\" + tableName + ".csv");
            var csv2 = new CsvReader(reader, CultureInfo.InvariantCulture);

            var records = csv2.GetRecords<GlobalUrbanPoint>().ToList();
            db.InsertMultipleRecords<GlobalUrbanPoint>(tableName, records);
            */


            //var watch = System.Diagnostics.Stopwatch.StartNew();
            ////Test Insert many to the data base
            //db.InsertMultipleRecords<GlobalUrbanPoint>(smallTable, syntheticData);
            //watch.Stop();
            //var elapsedMs = watch.Elapsed.TotalSeconds;
            //Console.WriteLine($"Create the new colection {smallTable} in {elapsedMs} sec");




            //////////////////////////////////////////////////////////////////////////////////////////////////////////
            ///Test bet queries: Find the courdinates for 1%,5%,20% of databese
            /*
            Thread[] threads = new Thread[NumThredsQueries];

            List<GlobalUrbanPoint> results2d1PerSent, results2d5PerSent, results2d20PerSent, results3d1PerSent, results3d5PerSent, results3d20PerSent;
            results2d1PerSent = results2d5PerSent = results2d20PerSent = results3d1PerSent = results3d5PerSent = results3d20PerSent = new List<GlobalUrbanPoint>();
            
            ///Find 1 5 and 20 per
            ///2d
            threads[0] = new Thread(() => { results2d1PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d1PerSent, lowY2d1PerSent, higX2d1PerSent, higY2d1PerSent); });
            threads[1] = new Thread(() => { results2d5PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d5PerSent, lowY2d5PerSent, higX2d5PerSent, higY2d5PerSent); });
            threads[2] = new Thread(() => { results2d20PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d20PerSent, lowY2d20PerSent, higX2d20PerSent, higY2d20PerSent); });
            /// 3d
            threads[3] = new Thread(() => { results3d1PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d1PerSent, lowY3d1PerSent, lowZ3d1PerSent, higX3d1PerSent, higY3d1PerSent, higZ3d1PerSent); });
            threads[4] = new Thread(() => { results3d5PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d5PerSent, lowY3d5PerSent, lowZ3d5PerSent, higX3d5PerSent, higY3d5PerSent, higZ3d5PerSent); });
            threads[5] = new Thread(() => { results3d20PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d20PerSent, lowY3d20PerSent, lowZ3d20PerSent, higX3d20PerSent, higY3d20PerSent, higZ3d20PerSent); });


            for (int i = 0; i < NumThredsQueries; i++)
            {
                threads[i].Start();
            }

            for (int i = 0; i < NumThredsQueries; i++)
            {
                threads[i].Join();
            }

            Console.WriteLine("For 2d querys");
            Console.WriteLine($" Count Results 1% {results2d1PerSent.Count}");
            Console.WriteLine($" Count Results 5% {results2d5PerSent.Count}");
            Console.WriteLine($" Count Results 20% {results2d20PerSent.Count}");

            Console.WriteLine("For 3d querys");
            Console.WriteLine($" Count Results 1% {results3d1PerSent.Count}");
            Console.WriteLine($" Count Results 5% {results3d5PerSent.Count}");
            Console.WriteLine($" Count Results 20% {results3d20PerSent.Count}");
            
            
            ///////////////////////////////////////////////////////////////////////////////////////////////////
            */


            //Theads that used for the creation of indexes
            Thread[] threads = new Thread[NumThredsIndex];

            ///////////////////////////////////////////////////////////////////////////////////////////////////
            
            for (int i = 1; i <= numExperemts; i++)
            {

                //Drop All Indexes from previous expiremnts
                db.DropAllIndexes<GlobalUrbanPoint>(tableName);
                // 2d Indexes
                double hilbert2dindexCreateTime, hibert2dDifineIndexTime, snake2dIndexCreateTime, snake2dDifineIndexTime, zOreder2dIndexCreateTime, zOreder2dDifineIndexTime;
                hilbert2dindexCreateTime = hibert2dDifineIndexTime = snake2dIndexCreateTime = snake2dDifineIndexTime = zOreder2dIndexCreateTime = zOreder2dDifineIndexTime = 0;

                

                threads[0] = new Thread(() => { hilbert2dindexCreateTime = db.AddHilbertIndex2DThread<GlobalUrbanPoint>(tableName); });
                threads[1] = new Thread(() => { snake2dIndexCreateTime = db.AddSnakeIndex2DThread<GlobalUrbanPoint>(tableName); });
                threads[2] = new Thread(() => { zOreder2dIndexCreateTime = db.AddZOrderIndex2DThread<GlobalUrbanPoint>(tableName); });

                for (int t = 0; t < NumThredsIndex; t++)
                {
                    threads[t].Start();
                }

                // Join The threads
                for (int t = 0; t < NumThredsIndex; t++)
                {
                    threads[t].Join();
                }

                // difine indexes
                var watch = System.Diagnostics.Stopwatch.StartNew();

                db.DefineHilbertIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();

                hibert2dDifineIndexTime = (double)watch.ElapsedMilliseconds;

                // Snake 2d

                if (!watch.IsRunning)
                    watch.Restart();

                db.DefineSnakeIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();

                snake2dDifineIndexTime = (double)watch.ElapsedMilliseconds;

                // Z order 2d

                if (!watch.IsRunning)
                    watch.Restart();

                db.DefineZOrderIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();

                zOreder2dDifineIndexTime = (double)watch.ElapsedMilliseconds;


                //Dubuge Console
                Console.WriteLine($"Hilbert index {hilbert2dindexCreateTime} \n" +
                                  $"Snake Index {snake2dIndexCreateTime}\n" +
                                  $"Zorder Index {zOreder2dIndexCreateTime}\n" +
                                  $"Difine Hilbert {hibert2dDifineIndexTime}\n" +
                                  $"Difine Snake {snake2dDifineIndexTime}\n" +
                                  $"Difine Zorder {zOreder2dDifineIndexTime}");
                
                // 2d Querys
                // courdinates 1 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dCourdinates1PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX2d1PerSent, lowY2d1PerSent, higX2d1PerSent, higY2d1PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 1% of data Count{rangeQuery2dCourdinates1PerSent.Count}");
                var time2dRangeQueryCourdinates1PerSent = (double)watch.ElapsedMilliseconds;

                //2d Query Hilber index 1 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dHilbert1PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX2d1PerSent, lowY2d1PerSent, higX2d1PerSent, higY2d1PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 1% of data Count{rangeQuery2dHilbert1PerSent.Count}");
                var time2dRangeQueryHilbert1PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Snake index 1 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dSnake1PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX2d1PerSent, lowY2d1PerSent, higX2d1PerSent, higY2d1PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 1% of data Count{rangeQuery2dSnake1PerSent.Count}");
                var time2dRangeQuerySnake1PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Zorder index 1 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dZorder1PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d1PerSent, lowY2d1PerSent, higX2d1PerSent, higY2d1PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 1% of data Count{rangeQuery2dZorder1PerSent.Count}");
                var time2dRangeQueryZorder1PerSent = (double)watch.ElapsedMilliseconds;


                (double precisionHilbert2d1PerSent, double recallHilbert2d1PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates1PerSent, rangeQuery2dHilbert1PerSent);
                (double precisionSnake2d1PerSent, double recallSnake2d1PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates1PerSent, rangeQuery2dSnake1PerSent);
                (double precisionZorder2d1PerSent, double recallZOrder2d1PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates1PerSent, rangeQuery2dZorder1PerSent);

                Console.WriteLine($"Precion and Recall for 2d 1 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert2d1PerSent} {recallHilbert2d1PerSent}");
                Console.WriteLine($"Snake {precisionSnake2d1PerSent} {recallSnake2d1PerSent}");
                Console.WriteLine($"Zorder {precisionZorder2d1PerSent} {recallZOrder2d1PerSent}");

                int count2dCourdinates1PerSent = rangeQuery2dCourdinates1PerSent.Count;
                int count2dHilbert1PerSent = rangeQuery2dHilbert1PerSent.Count;
                int count2dSnake1PerSent = rangeQuery2dSnake1PerSent.Count;
                int count2dZorder1PerSent = rangeQuery2dZorder1PerSent.Count;

                //Free the unused memorry GC
                rangeQuery2dCourdinates1PerSent = rangeQuery2dHilbert1PerSent = rangeQuery2dSnake1PerSent = rangeQuery2dZorder1PerSent = null;


                // 2d Querys courdinates 5 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dCourdinates5PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX2d5PerSent, lowY2d5PerSent, higX2d5PerSent, higY2d5PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 5% of data Count {rangeQuery2dCourdinates5PerSent.Count}");
                var time2dRangeQueryCourdinates5PerSent = (double)watch.ElapsedMilliseconds;

                //2d Query Hilber index 5 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dHilbert5PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX2d5PerSent, lowY2d5PerSent, higX2d5PerSent, higY2d5PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 5% of data Count {rangeQuery2dHilbert5PerSent.Count}");
                var time2dRangeQueryHilbert5PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Snake index 5 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dSnake5PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX2d5PerSent, lowY2d5PerSent, higX2d5PerSent, higY2d5PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 5% of data Count {rangeQuery2dSnake5PerSent.Count}");
                var time2dRangeQuerySnake5PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Zorder index 5 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dZorder5PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d5PerSent, lowY2d5PerSent, higX2d5PerSent, higY2d5PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 1% of data Count{rangeQuery2dZorder5PerSent.Count}");
                var time2dRangeQueryZorder5PerSent = (double)watch.ElapsedMilliseconds;

                (double precisionHilbert2d5PerSent, double recallHilbert2d5PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates5PerSent, rangeQuery2dHilbert5PerSent);
                (double precisionSnake2d5PerSent, double recallSnake2d5PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates5PerSent, rangeQuery2dSnake5PerSent);
                (double precisionZorder2d5PerSent, double recallZOrder2d5PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates5PerSent, rangeQuery2dZorder5PerSent);

                Console.WriteLine($"Precion and Recall for 2d 5 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert2d5PerSent} {recallHilbert2d5PerSent}");
                Console.WriteLine($"Snake {precisionSnake2d5PerSent} {recallSnake2d5PerSent}");
                Console.WriteLine($"Zorder {precisionZorder2d5PerSent} {recallZOrder2d5PerSent}");

                int count2dCourdinates5PerSent = rangeQuery2dCourdinates5PerSent.Count;
                int count2dHilbert5PerSent = rangeQuery2dHilbert5PerSent.Count;
                int count2dSnake5PerSent = rangeQuery2dSnake5PerSent.Count;
                int count2dZorder5PerSent = rangeQuery2dZorder5PerSent.Count;

                rangeQuery2dCourdinates5PerSent = rangeQuery2dHilbert5PerSent = rangeQuery2dSnake5PerSent = rangeQuery2dZorder5PerSent = null;

                
                // 2d Querys courdinates 20 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dCourdinates20PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX2d20PerSent, lowY2d20PerSent, higX2d20PerSent, higY2d20PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 20% of data Count {rangeQuery2dCourdinates20PerSent.Count}");
                var time2dRangeQueryCourdinates20PerSent = (double)watch.ElapsedMilliseconds;

                //2d Query Hilber index 20 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dHilbert20PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX2d20PerSent, lowY2d20PerSent, higX2d20PerSent, higY2d20PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 20% of data Count {rangeQuery2dHilbert20PerSent.Count}");
                var time2dRangeQueryHilbert20PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Snake index 20 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dSnake20PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX2d20PerSent, lowY2d20PerSent, higX2d20PerSent, higY2d20PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Results From 20% of data Count {rangeQuery2dSnake20PerSent.Count}");
                var time2dRangeQuerySnake20PerSent = (double)watch.ElapsedMilliseconds;

                // 2d Query Zorder index 20 persent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery2dZorder20PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX2d20PerSent, lowY2d20PerSent, higX2d20PerSent, higY2d20PerSent);

                watch.Stop();
                Console.WriteLine($"Results From 20% of data Count{rangeQuery2dZorder20PerSent.Count}");
                var time2dRangeQueryZorder20PerSent = (double)watch.ElapsedMilliseconds;


                (double precisionHilbert2d20PerSent, double recallHilbert2d20PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates20PerSent, rangeQuery2dHilbert20PerSent);
                (double precisionSnake2d20PerSent, double recallSnake2d20PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates20PerSent, rangeQuery2dSnake20PerSent);
                (double precisionZorder2d20PerSent, double recallZOrder2d20PerSent) = PrecisionAndRecallThread(rangeQuery2dCourdinates20PerSent, rangeQuery2dZorder20PerSent);

                Console.WriteLine($"Precion and Recall for 2d 20 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert2d20PerSent} {recallHilbert2d20PerSent}");
                Console.WriteLine($"Snake {precisionSnake2d20PerSent} {recallSnake2d20PerSent}");
                Console.WriteLine($"Zorder {precisionZorder2d20PerSent} {recallZOrder2d20PerSent}");

                int count2dCourdinates20PerSent = rangeQuery2dCourdinates20PerSent.Count;
                int count2dHilbert20PerSent = rangeQuery2dHilbert20PerSent.Count;
                int count2dSnake20PerSent = rangeQuery2dSnake20PerSent.Count;
                int count2dZorder20PerSent = rangeQuery2dZorder20PerSent.Count;

                rangeQuery2dCourdinates20PerSent = rangeQuery2dHilbert20PerSent = rangeQuery2dSnake20PerSent = rangeQuery2dZorder20PerSent = null;
                
                // Reset variables
                db.DropAllIndexes<GlobalUrbanPoint>(tableName);
                db.ResetIndexVariables<GlobalUrbanPoint>(tableName);

                // Add 3d indexies
                double hilbert3dindexCreateTime, hibert3dDifineIndexTime, snake3dIndexCreateTime, snake3dDifineIndexTime, zOreder3dIndexCreateTime, zOreder3dDifineIndexTime;
                hilbert3dindexCreateTime = hibert3dDifineIndexTime = snake3dIndexCreateTime = snake3dDifineIndexTime = zOreder3dIndexCreateTime = zOreder3dDifineIndexTime = 0;



                threads[0] = new Thread(() => { hilbert3dindexCreateTime = db.AddHilbertIndex3DThread<GlobalUrbanPoint>(tableName); });
                threads[1] = new Thread(() => { snake3dIndexCreateTime = db.AddSnakeIndex3DThread<GlobalUrbanPoint>(tableName); });
                threads[2] = new Thread(() => { zOreder3dIndexCreateTime = db.AddZOrderIndex3DThread<GlobalUrbanPoint>(tableName); });

                for (int t = 0; t < NumThredsIndex; t++)
                {
                    threads[t].Start();
                }

                // Join The threads
                for (int t = 0; t < NumThredsIndex; t++)
                {
                    threads[t].Join();
                }

                /// DEDINE INDEX ///
               
                // Hilbert 3d
                if (!watch.IsRunning)
                    watch.Restart();

                db.DefineHilbertIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();
                hibert3dDifineIndexTime = (double)watch.ElapsedMilliseconds;

                // Snake 3d
                if (!watch.IsRunning)
                    watch.Restart();

                db.DefineSnakeIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();
                snake3dDifineIndexTime = (double)watch.ElapsedMilliseconds;

                // Z order 3d
                if (!watch.IsRunning)
                    watch.Restart();

                db.DefineZOrderIndex<GlobalUrbanPoint>(tableName);

                watch.Stop();
                zOreder3dDifineIndexTime = (double)watch.ElapsedMilliseconds;

                /// 3d Experiments ///

                // 3d Querys courdinates 1 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dCourdinates1PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX3d1PerSent, lowY3d1PerSent, lowZ3d1PerSent, higX3d1PerSent, higY3d1PerSent, higZ3d1PerSent);

                watch.Stop();
                Console.WriteLine($"Querry Courdinates Results 1% of data Count {rangeQuery3dCourdinates1PerSent.Count}");
                var time3dRangeQueryCourdinates1PerSent = (double)watch.ElapsedMilliseconds;


                /// 3d queries Hilbert 1 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dHilbert1PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX3d1PerSent, lowY3d1PerSent, lowZ3d1PerSent, higX3d1PerSent, higY3d1PerSent, higZ3d1PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Hilbert Results 1% of data Count {rangeQuery3dHilbert1PerSent.Count}");
                var time3dRangeQueryHilbert1PerSent = (double)watch.ElapsedMilliseconds;

                // 3d queries Snake 1 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dSnake1PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX3d1PerSent, lowY3d1PerSent, lowZ3d1PerSent, higX3d1PerSent, higY3d1PerSent, higZ3d1PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Snake Results 1% of data Count {rangeQuery3dSnake1PerSent.Count}");
                var time3dRangeQuerySnake1PerSent = (double)watch.ElapsedMilliseconds;

                // 3d queries Z order 1 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dZorder1PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d1PerSent, lowY3d1PerSent, lowZ3d1PerSent, higX3d1PerSent, higY3d1PerSent, higZ3d1PerSent);

                watch.Stop();
                Console.WriteLine($"Querry Zorder Results 1% of data Count {rangeQuery3dZorder1PerSent.Count}");
                var time3dRangeQueryZorder1PerSent = (double)watch.ElapsedMilliseconds;

                (double precisionHilbert3d1PerSent, double recallHilbert3d1PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates1PerSent, rangeQuery3dHilbert1PerSent);
                (double precisionSnake3d1PerSent, double recallSnake3d1PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates1PerSent, rangeQuery3dSnake1PerSent);
                (double precisionZorder3d1PerSent, double recallZOrder3d1PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates1PerSent, rangeQuery3dZorder1PerSent);

                Console.WriteLine($"Precion and Recall for 3d 1 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert3d1PerSent} {recallHilbert3d1PerSent}");
                Console.WriteLine($"Snake {precisionSnake3d1PerSent} {recallSnake3d1PerSent}");
                Console.WriteLine($"Zorder {precisionZorder3d1PerSent} {recallZOrder3d1PerSent}");


                int count3dCourdinates1PerSent = rangeQuery3dCourdinates1PerSent.Count;
                int count3dHilbert1PerSent = rangeQuery3dHilbert1PerSent.Count;
                int count3dSnake1PerSent = rangeQuery3dSnake1PerSent.Count;
                int count3dZorder1PerSent = rangeQuery3dZorder1PerSent.Count;

                rangeQuery3dCourdinates1PerSent = rangeQuery3dHilbert1PerSent = rangeQuery3dSnake1PerSent = rangeQuery3dZorder1PerSent = null;

                // 3d Query courdinates 5 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dCourdinates5PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX3d5PerSent, lowY3d5PerSent, lowZ3d5PerSent, higX3d5PerSent, higY3d5PerSent, higZ3d5PerSent);

                watch.Stop();
                Console.WriteLine($"Querry Courdinates Results 5% of data Count {rangeQuery3dCourdinates5PerSent.Count}");
                var time3dRangeQueryCourdinates5PerSent = (double)watch.ElapsedMilliseconds;

                // 3d queries Hilbert 5 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dHilbert5PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX3d5PerSent, lowY3d5PerSent, lowZ3d5PerSent, higX3d5PerSent, higY3d5PerSent, higZ3d5PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Hilbert Results 5% of data Count {rangeQuery3dHilbert5PerSent.Count}");
                var time3dRangeQueryHilbert5PerSent = (double)watch.ElapsedMilliseconds;

                //3d queries Snake 5 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dSnake5PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX3d5PerSent, lowY3d5PerSent, lowZ3d5PerSent, higX3d5PerSent, higY3d5PerSent, higZ3d5PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Snake Results 5% of data Count {rangeQuery3dSnake5PerSent.Count}");
                var time3dRangeQuerySnake5PerSent = (double)watch.ElapsedMilliseconds;

                // 3d queries Z order 5 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dZorder5PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d5PerSent, lowY3d5PerSent, lowZ3d5PerSent, higX3d5PerSent, higY3d5PerSent, higZ3d5PerSent);
                
                watch.Stop();
                Console.WriteLine($"Querry Zorder Results 5% of data Count {rangeQuery3dZorder5PerSent.Count}");
                var time3dRangeQueryZorder5PerSent = (double)watch.ElapsedMilliseconds;

                (double precisionHilbert3d5PerSent, double recallHilbert3d5PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates5PerSent, rangeQuery3dHilbert5PerSent);
                (double precisionSnake3d5PerSent, double recallSnake3d5PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates5PerSent, rangeQuery3dSnake5PerSent);
                (double precisionZorder3d5PerSent, double recallZOrder3d5PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates5PerSent, rangeQuery3dZorder5PerSent);

                Console.WriteLine($"Precion and Recall for 3d 1 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert3d5PerSent} {recallHilbert3d5PerSent}");
                Console.WriteLine($"Snake {precisionSnake3d5PerSent} {recallSnake3d5PerSent}");
                Console.WriteLine($"Zorder {precisionZorder3d5PerSent} {recallZOrder3d5PerSent}");

                int count3dCourdinates5PerSent = rangeQuery3dCourdinates5PerSent.Count;
                int count3dHilbert5PerSent = rangeQuery3dHilbert5PerSent.Count;
                int count3dSnake5PerSent = rangeQuery3dSnake5PerSent.Count;
                int count3dZorder5PerSent = rangeQuery3dZorder5PerSent.Count;

                rangeQuery3dCourdinates5PerSent = rangeQuery3dHilbert5PerSent = rangeQuery3dSnake5PerSent = rangeQuery3dZorder5PerSent = null;

                // 3d queries courdinates 20 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dCourdinates20PerSent = db.RangeQueryCodrinates<GlobalUrbanPoint>(tableName, lowX3d20PerSent, lowY3d20PerSent, lowZ3d20PerSent, higX3d20PerSent, higY3d20PerSent, higZ3d20PerSent);

                watch.Stop();
                Console.WriteLine($"Querry Courdinates Results 20% of data Count {rangeQuery3dCourdinates20PerSent.Count}");
                var time3dRangeQueryCourdinates20PerSent = (double)watch.ElapsedMilliseconds;


                // 3d queries Hilbert 20 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dHilbert20PerSent = db.RangeQueryHilbert<GlobalUrbanPoint>(tableName, lowX3d20PerSent, lowY3d20PerSent, lowZ3d20PerSent, higX3d20PerSent, higY3d20PerSent, higZ3d20PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Hilbert Results 20% of data Count {rangeQuery3dHilbert20PerSent.Count}");
                var time3dRangeQueryHilbert20PerSent = (double)watch.ElapsedMilliseconds;

                //3d queries Snake 20 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dSnake20PerSent = db.RangeQuerySnake<GlobalUrbanPoint>(tableName, lowX3d20PerSent, lowY3d20PerSent, lowZ3d20PerSent, higX3d20PerSent, higY3d20PerSent, higZ3d20PerSent, bits);

                watch.Stop();
                Console.WriteLine($"Querry Snake Results 20% of data Count {rangeQuery3dSnake20PerSent.Count}");
                var time3dRangeQuerySnake20PerSent = (double)watch.ElapsedMilliseconds;

                // 3d queries Z order 20 per sent
                if (!watch.IsRunning)
                    watch.Restart();

                var rangeQuery3dZorder20PerSent = db.RangeQueryZOreder<GlobalUrbanPoint>(tableName, lowX3d20PerSent, lowY3d20PerSent, lowZ3d20PerSent, higX3d20PerSent, higY3d20PerSent, higZ3d20PerSent);

                watch.Stop();
                Console.WriteLine($"Querry Zorder Results 20% of data Count {rangeQuery3dZorder20PerSent.Count}");
                var time3dRangeQueryZorder20PerSent = (double)watch.ElapsedMilliseconds;

                (double precisionHilbert3d20PerSent, double recallHilbert3d20PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates20PerSent, rangeQuery3dHilbert20PerSent);
                (double precisionSnake3d20PerSent, double recallSnake3d20PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates20PerSent, rangeQuery3dSnake20PerSent);
                (double precisionZorder3d20PerSent, double recallZOrder3d20PerSent) = PrecisionAndRecallThread(rangeQuery3dCourdinates20PerSent, rangeQuery3dZorder20PerSent);

                int count3dCourdinates20PerSent = rangeQuery3dCourdinates20PerSent.Count;
                int count3dHilbert20PerSent = rangeQuery3dHilbert20PerSent.Count;
                int count3dSnake20PerSent = rangeQuery3dSnake20PerSent.Count;
                int count3dZorder20PerSent = rangeQuery3dZorder20PerSent.Count;

                //Free The Memmory from the queries
                rangeQuery3dCourdinates20PerSent = rangeQuery3dHilbert20PerSent = rangeQuery3dSnake20PerSent = rangeQuery3dZorder20PerSent = null;

                Console.WriteLine($"Precion and Recall for 3d 1 Per Sent");
                Console.WriteLine($"Hilber {precisionHilbert3d20PerSent} {recallHilbert3d20PerSent}");
                Console.WriteLine($"Snake {precisionSnake3d20PerSent} {recallSnake3d20PerSent}");
                Console.WriteLine($"Zorder {precisionZorder3d20PerSent} {recallZOrder3d20PerSent}");

                // Reset variables
                db.DropAllIndexes<GlobalUrbanPoint>(tableName);
                db.ResetIndexVariables<GlobalUrbanPoint>(tableName);

                // pass all mesures to a csv Report
                var Record = new List<FormatCsv> { new FormatCsv {
                            // Add and Difined 2d Indexes
                            TimeAddHilberd2d = hilbert2dindexCreateTime,
                            TimeDifindedHilberd2d = hibert2dDifineIndexTime,
                            TimeAddSnake2d = snake2dIndexCreateTime,
                            TimeDifinedSnake2d = snake2dDifineIndexTime,
                            TimeAddZOrder2d = zOreder2dIndexCreateTime,
                            TimeDifinedZOrder2d = zOreder2dDifineIndexTime,

                            // 2d queres 1 per sent
                            TimeQueryCourdinates2d1Percent = time2dRangeQueryCourdinates1PerSent,
                            DocumetsRetreveQuerryCourdinates2d1Percent = count2dCourdinates1PerSent,
                            TimeQueryHilber2d1Percent = time2dRangeQueryHilbert1PerSent,
                            DocumetsRetreveQuerryHilbert2d1Percent = count2dHilbert1PerSent,
                            RecallHilbert2d1Percent = recallHilbert2d1PerSent,
                            PrecisionHilbert2d1Percent = precisionHilbert2d1PerSent,
                            TimeQuerySnake2d1Percent = time2dRangeQuerySnake1PerSent,
                            DocumetsRetreveQuerrySnake2d1Percent = count2dSnake1PerSent,
                            RecallSnake2d1Percent = recallSnake2d1PerSent,
                            PrecisionSbake2d1Percent = precisionSnake2d1PerSent,
                            TimeQueryZorder2d1Percent = time2dRangeQueryZorder1PerSent,
                            DocumetsRetreveQuerryZorder2d1Percent = count2dZorder1PerSent,
                            RecallZorder2d1Percent = recallZOrder2d1PerSent,
                            PrecisionZorder2d1Percent = precisionZorder2d1PerSent,

                            // 2d queres 5 per sent
                            TimeQueryCourdinates2d5Percent =  time2dRangeQueryCourdinates5PerSent,
                            DocumetsRetreveQuerryCourdinates2d5Percent = count2dCourdinates5PerSent,
                            TimeQueryHilber2d5Percent = time2dRangeQueryHilbert5PerSent,
                            DocumetsRetreveQuerryHilbert2d5Percent = count2dHilbert5PerSent,
                            RecallHilbert2d5Percent = recallHilbert2d5PerSent,
                            PrecisionHilbert2d5Percent = precisionHilbert2d5PerSent,
                            TimeQuerySnake2d5Percent = time2dRangeQuerySnake5PerSent,
                            DocumetsRetreveQuerrySnake2d5Percent = count2dSnake5PerSent,
                            RecallSnake2d5Percent = recallSnake2d5PerSent,
                            PrecisionSbake2d5Percent = precisionSnake2d5PerSent,
                            TimeQueryZorder2d5Percent = time2dRangeQueryZorder5PerSent,
                            DocumetsRetreveQuerryZorder2d5Percent = count2dZorder5PerSent,
                            RecallZorder2d5Percent = recallZOrder2d5PerSent,
                            PrecisionZorder2d5Percent = precisionZorder2d5PerSent,
                            
                            // 2d queres 20 per sent
                            TimeQueryCourdinates2d20Percent = time2dRangeQueryCourdinates20PerSent,
                            DocumetsRetreveQuerryCourdinates2d20Percent = count2dCourdinates20PerSent,
                            TimeQueryHilber2d20Percent = time2dRangeQueryHilbert20PerSent,
                            DocumetsRetreveQuerryHilbert2d20Percent = count2dHilbert20PerSent,
                            RecallHilbert2d20Percent = recallHilbert2d20PerSent,
                            PrecisionHilbert2d20Percent = precisionHilbert2d20PerSent,
                            TimeQuerySnake2d20Percent = time2dRangeQuerySnake20PerSent,
                            DocumetsRetreveQuerrySnake2d20Percent = count2dSnake20PerSent,
                            RecallSnake2d20Percent = recallSnake2d20PerSent,
                            PrecisionSbake2d20Percent = precisionSnake2d20PerSent,
                            TimeQueryZorder2d20Percent = time2dRangeQueryZorder20PerSent,
                            DocumetsRetreveQuerryZorder2d20Percent = count2dZorder20PerSent,
                            RecallZorder2d20Percent = recallZOrder2d20PerSent,
                            PrecisionZorder2d20Percent = precisionZorder2d20PerSent,

                            // Add and Difined 3d Indexes
                            TimeAddHilberd3d = hilbert3dindexCreateTime,
                            TimeDifindedHilberd3d = hibert3dDifineIndexTime,
                            TimeAddSnake3d = snake3dIndexCreateTime,
                            TimeDifinedSnake3d = snake3dDifineIndexTime,
                            TimeAddZOrder3d = zOreder3dIndexCreateTime,
                            TimeDifinedZOrder3d = zOreder3dDifineIndexTime,

                            // 3d queres 1 per sent
                            TimeQueryCourdinates3d1Percent = time3dRangeQueryCourdinates1PerSent,
                            DocumetsRetreveQuerryCourdinates3d1Percent = count3dCourdinates1PerSent,
                            TimeQueryHilber3d1Percent = time3dRangeQueryHilbert1PerSent,
                            DocumetsRetreveQuerryHilbert3d1Percent = count3dHilbert1PerSent,
                            RecallHilbert3d1Percent = recallHilbert3d1PerSent,
                            PrecisionHilbert3d1Percent = precisionHilbert3d1PerSent,
                            TimeQuerySnake3d1Percent = time3dRangeQuerySnake1PerSent,
                            DocumetsRetreveQuerrySnake3d1Percent = count3dSnake1PerSent,
                            RecallSnake3d1Percent = recallSnake3d1PerSent,
                            PrecisionSbake3d1Percent = precisionSnake3d1PerSent,
                            TimeQueryZorder3d1Percent = time3dRangeQueryZorder1PerSent,
                            DocumetsRetreveQuerryZorder3d1Percent = count3dZorder1PerSent,
                            RecallZorder3d1Percent = recallZOrder3d1PerSent,
                            PrecisionZorder3d1Percent = precisionZorder3d1PerSent,

                            // 3d queres 5 per sent
                            TimeQueryCourdinates3d5Percent = time3dRangeQueryCourdinates5PerSent,
                            DocumetsRetreveQuerryCourdinates3d5Percent = count3dCourdinates5PerSent,
                            TimeQueryHilber3d5Percent = time3dRangeQueryHilbert5PerSent,
                            DocumetsRetreveQuerryHilbert3d5Percent = count3dHilbert5PerSent,
                            RecallHilbert3d5Percent = recallHilbert3d5PerSent,
                            PrecisionHilbert3d5Percent = precisionHilbert3d5PerSent,
                            TimeQuerySnake3d5Percent = time3dRangeQuerySnake5PerSent,
                            DocumetsRetreveQuerrySnake3d5Percent = count3dSnake5PerSent,
                            RecallSnake3d5Percent = recallSnake3d5PerSent,
                            PrecisionSbake3d5Percent = precisionSnake3d5PerSent,
                            TimeQueryZorder3d5Percent = time3dRangeQueryZorder5PerSent,
                            DocumetsRetreveQuerryZorder3d5Percent = count3dZorder5PerSent,
                            RecallZorder3d5Percent = recallZOrder3d5PerSent,
                            PrecisionZorder3d5Percent = precisionZorder3d1PerSent,

                            // 3d queres 20 per sent
                            TimeQueryCourdinates3d20Percent = time3dRangeQueryCourdinates20PerSent,
                            DocumetsRetreveQuerryCourdinates3d20Percent = count3dCourdinates20PerSent,
                            TimeQueryHilber3d20Percent = time3dRangeQueryHilbert20PerSent,
                            DocumetsRetreveQuerryHilbert3d20Percent = count3dHilbert20PerSent,
                            RecallHilbert3d20Percent = recallHilbert3d20PerSent,
                            PrecisionHilbert3d20Percent = precisionHilbert3d20PerSent,
                            TimeQuerySnake3d20Percent = time3dRangeQuerySnake20PerSent,
                            DocumetsRetreveQuerrySnake3d20Percent = count3dSnake20PerSent,
                            RecallSnake3d20Percent = recallSnake3d20PerSent,
                            PrecisionSbake3d20Percent = precisionSnake3d20PerSent,
                            TimeQueryZorder3d20Percent = time3dRangeQueryZorder20PerSent,
                            DocumetsRetreveQuerryZorder3d20Percent = count3dZorder20PerSent,
                            RecallZorder3d20Percent = recallZOrder3d20PerSent,
                            PrecisionZorder3d20Percent = precisionZorder3d20PerSent   
                } };

                var writer = new StreamWriter("D:\\Desktop\\CSV_Data\\Results\\"+tableName+"Exp"+i.ToString()+".csv");
                var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
                csv.WriteRecords<FormatCsv>(Record);
                writer.Flush();

                Console.WriteLine($"================== I have finish the {i} expiremts ==================");

                db.DropAllIndexes<GlobalUrbanPoint>(tableName);
                db.ResetIndexVariables<GlobalUrbanPoint>(tableName);
            }
            
            
        }

        public static IEnumerable<T> Flatten<T>(T[,] items)
        {
            for (int i = 0; i < items.GetLength(0); i++)
                for (int j = 0; j < items.GetLength(1); j++)
                    yield return items[i, j];
        }

        // It is out of main to have more uniform destribution
        static Random rndg = new Random();

        // Random strings
        public static string RandomString(int lengthOfString)
        {
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var stringChars = new char[lengthOfString];
            var random = new Random();

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(chars.Length)];
            }

            var finalString = new String(stringChars);
            return finalString;
        }

        //Random Day form calender
        static Func<DateTime> RandomDayFunc()
        {
            DateTime start = new DateTime(1940, 1, 1);
            Random gen = new Random();
            int range = ((TimeSpan)(DateTime.Today - start)).Days;
            return () => start.AddDays(gen.Next(range));
        }

        //Random Contetinet
        public static string PickContinet()
        {
            List<string> ListContinet = new List<string>(new string[] { "Asia", "Africa", "North America", "South America",
                                                                        "Antarctica", "Europe", "Australia" });
            int r = rndg.Next(ListContinet.Count);
            return ListContinet[r];
        }

        //Random Laguege
        public static string PickLanguege()
        {
            List<string> ListLanguage = new List<string>(new string[] { "Afrikaans","Albanian","Amharic","Arabic",
                                                                        "Aramaic","Armenian","Assamese","Aymara",
                                                                        "Azerbaijani","Balochi","Bamanankan",
                                                                        "Bashkort","Basque","Belarusan","Bengali",
                                                                        "Bhojpuri","Bislama","Bosnian","Brahui",
                                                                        "Bulgarian","Burmese","Cantonese","Catalan",
                                                                        "Cebuano","Chechen","Cherokee","Croatian",
                                                                        "Czech","Dakota","Danish","Dari","Dholuo",
                                                                        "Dutch","English","Esperanto","Estonian",
                                                                        "Éwé","Finnish","French","Georgian","German",
                                                                        "Gikuyu","Greek","Guarani","Gujarati","Haitian Creole",
                                                                        "Hausa","Hawaiian","Hebrew","Hiligaynon","Hindi",
                                                                        "Hungarian","Icelandic","Igbo","Ilocano","Indonesian",
                                                                        "Irish Gaelic","Italian","Japanese","Jarai","Javanese",
                                                                        "K’iche’","Kabyle","Kannada","Kashmiri","Kazakh","Khmer",
                                                                        "Khoekhoe","Korean","Kurdish","Kyrgyz","Lao","Latin",
                                                                        "Latvian","Lingala","Lithuanian","Macedonian","Maithili",
                                                                        "Malagasy","Malay","Malayalam","Mandarin","Marathi","Mende",
                                                                        "Mongolian","Nahuatl","Navajo","Nepali","Norwegian","Ojibwa",
                                                                        "Oriya","Oromo","Pashto","Persian","Polish","Portuguese",
                                                                        "Punjabi","Quechua","Romani","Romanian","Russian","Rwanda",
                                                                        "Samoan","Sanskrit","Serbian","Shona","Sindhi","Sinhala",
                                                                        "Slovak","Slovene","Somali","Spanish","Swahili","Swedish",
                                                                        "Tachelhit","Tagalog","Tajiki","Tamil","Tatar","Telugu",
                                                                        "Thai","Tigrigna","Tok Pisin","Turkish","Turkmen",
                                                                        "Ukrainian","Urdu","Uyghur","Uzbek","Vietnamese",
                                                                        "Warlpiri","Welsh","Wolof","Xhosa","Yakut","Yiddish",
                                                                        "Yoruba","Yucatec","Zapotec","Zulu"});
            int r = rndg.Next(ListLanguage.Count);
            return ListLanguage[r];
        }

        // Random Type
        public static string PickType()
        {
            List<string> ListType = new List<string>(new string[] { "C", "Village" });
            int r = rndg.Next(ListType.Count);
            return ListType[r];

        }

        //Ramdom Region
        public static string PickUNREGION(string continet)
        {
            List<string> ListEurop = new List<string>(new string[] { "EASTERN EUROPE", "NORTHERN EUROPE", "SOUTHERN EUROPE",
                                                                     "WESTERN EUROPE" });
            List<string> ListAsia = new List<string>(new string[] { "Central Asia", "Eastern Asia", "North America", "South America",
                                                                    "South-Eastern Asia", "Western Asia" });
            List<string> ListAfrica = new List<string>(new string[] { "Eastern Africa", "Middle Africa", "Northern Africa", "Southern Africa",
                                                                       "Western Africa" });
            List<string> ListAustralia = new List<string>(new string[] { "AUSTRALIA AND NEW ZEALAND", "MELANESIA",
                                                                         "MICRONESIA", "POLYNESIA" });
            List<string> ListNorthAmerica = new List<string>(new string[] { "Central America", "Northern America" });
            List<string> ListSouthAmerica = new List<string>(new string[] { "Caribbean", "South America" });
            List<string> ListContinet = new List<string>(new string[] { "Asia", "Africa", "North America", "South America",
                                                                        "Europe","Australia", "Antarctica" });

            int r;

            if (continet == ListContinet[0])
            {
                r = rndg.Next(ListAsia.Count);
                return ListAsia[r];
            }
            else if (continet == ListContinet[1])
            {
                r = rndg.Next(ListAfrica.Count);
                return ListAfrica[r];
            }
            else if (continet == ListContinet[2])
            {
                r = rndg.Next(ListNorthAmerica.Count);
                return ListNorthAmerica[r];
            }
            else if (continet == ListContinet[3])
            {
                r = rndg.Next(ListSouthAmerica.Count);
                return ListSouthAmerica[r];
            }
            else if (continet == ListContinet[4])
            {
                r = rndg.Next(ListEurop.Count);
                return ListEurop[r];
            }
            else if (continet == ListContinet[5])
            {
                r = rndg.Next(ListAustralia.Count);
                return ListAustralia[r];
            }
            else
            {
                return "Antarctica";
            }


        }


        /// <summary>
        /// Create synthetic data with Uniform Distribition
        /// </summary>
        /// <param name="NumberOfDocumet"></param>
        /// <returns> List of GlobalUrbanPoint</returns> 
        public static List<GlobalUrbanPoint> CreateSunfeticData(int NumberOfDocumets)
        {
            List<GlobalUrbanPoint> SyntheticList = new List<GlobalUrbanPoint>();

            //Variables for random staf
            var getRandomDate = RandomDayFunc();
            var randomDate = getRandomDate();
            //Random Population
            Random rnd = new Random();

            // Test math.net
            var random = new MersenneTwister(13);

            //int maxCordinate = (int)Math.Round(Math.Sqrt((double)NumberOfDocumet));

            // digits of courdinates
            int numDigids = 10000;

            // hash cordinates
            HashSet<string> hashSet = new HashSet<string>();
            string tempLat = "";
            string tempLon = "";
            string tempHig = "";
            string finalHash = "";


            for (var i = 0; i < NumberOfDocumets; i++)
            {
                var SyntheticObject = new GlobalUrbanPoint();



                // Change random year
                randomDate = getRandomDate();

                // The range of coordinates classical randiom numbers
                //SyntheticObject.LATITUDE = rnd.Next(0,10000);
                //SyntheticObject.LONGITUDE = rnd.Next(0, 10000);

                /// Generate Uniform Random Coordinates 
                while (true)
                {
                    SyntheticObject.LATITUDE = (int)(random.NextDecimal() * numDigids);
                    SyntheticObject.LONGITUDE = (int)(random.NextDecimal() * numDigids);
                    SyntheticObject.HIGTH = (int)(random.NextDecimal() * numDigids);
                    tempLat = SyntheticObject.LATITUDE.ToString();
                    tempLon = SyntheticObject.LONGITUDE.ToString();
                    tempHig = SyntheticObject.HIGTH.ToString();

                    if (tempLat.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";
                        for (int iL = 0; iL < numDigids.ToString().Length - tempLat.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempLat = addZeros + tempLat;
                    }

                    if (tempLon.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";

                        for (int iL = 0; iL < numDigids.ToString().Length - tempLon.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempLon = addZeros + tempLon;
                    }

                    if (tempHig.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";

                        for (int iL = 0; iL < numDigids.ToString().Length - tempHig.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempHig = addZeros + tempHig;
                    }



                    finalHash = tempLat + "," + tempLon + "," + tempHig;

                    if (hashSet.Contains(finalHash) == false)
                    {
                        //                      Console.WriteLine($"Hash string {finalHash}");
                        hashSet.Add(finalHash);
                        break;
                    }

                }

                SyntheticObject.OBJECTID = i;
                SyntheticObject.URBORRUR = RandomString(1);
                SyntheticObject.YEAR_ = randomDate.Year;
                SyntheticObject.ES90POP = (uint)rnd.Next(1000, 10000000);
                SyntheticObject.ES95POP = SyntheticObject.ES90POP + (uint)rnd.Next(20000, 10000000);
                SyntheticObject.ES00POP = SyntheticObject.ES95POP + (uint)(((20.0 / 100.0)) * (double)SyntheticObject.ES95POP);
                SyntheticObject.CONTINENT = PickContinet();
                SyntheticObject.UNREGION = PickUNREGION(SyntheticObject.CONTINENT);
                SyntheticObject.COUNTRY = RandomString(8);
                SyntheticObject.FORGNNM = PickLanguege();
                SyntheticObject.ISO3 = RandomString(3);
                SyntheticObject.UNSD = rnd.Next(1000);
                SyntheticObject.SCHADMNM = RandomString(6);
                SyntheticObject.NAME1 = SyntheticObject.SCHADMNM;
                SyntheticObject.TYPE = PickType();
                SyntheticObject.SCHADMNM = RandomString(8);
                SyntheticObject.ADMNM1 = RandomString(6);
                SyntheticObject.ADMNM2 = SyntheticObject.ADMNM1;
                SyntheticObject.SRCTYP = RandomString(9);
                SyntheticObject.NOTES = RandomString(150);
                SyntheticObject.COORDSRCE = RandomString(4);
                SyntheticObject.LOCNDATSRC = RandomString(40);
                SyntheticObject.DATSRC = RandomString(120);
                SyntheticObject.INSGRUSED = "N";
                SyntheticObject.UNREGION = SyntheticObject.COUNTRY;

                SyntheticList.Add(SyntheticObject);
            }
            Console.WriteLine($"Intemes in list {SyntheticList.Count}");

            foreach (var item in SyntheticList)
            {
                item.LATITUDE = ConvertRange(0.0, (double)numDigids, 0, 8191, item.LATITUDE);
                item.LONGITUDE = ConvertRange(0.0, (double)numDigids, 0, 8191, item.LONGITUDE);
                item.HIGTH = ConvertRange(0.0, (double)numDigids, 0, 8191, item.HIGTH);
            }


            return SyntheticList;
        }

        /// <summary>
        /// Create synthetic data with Non Uniform Distribition (Laplace Distribusion)
        /// </summary>
        /// <param name="NumberOfDocuments"></param>
        /// <param name="trueChance"> 
        /// Form 0 to 100 if it 100 then the function use for all documents the uniform distridution.
        /// If trueChance is 0 then they use the Laplasion distridution
        /// </param>
        /// <returns></returns>
        public static List<GlobalUrbanPoint> CreateSunfeticDataNonUniform(int NumberOfDocuments, int trueChance, int bits)
        {
            List<GlobalUrbanPoint> SyntheticList = new List<GlobalUrbanPoint>();

            //For Bias Data
            Random randGen = new Random();
           
            int propability = 0;

            //Variables for random staf
            var getRandomDate = RandomDayFunc();
            var randomDate = getRandomDate();
            //Random Population
            Random rnd = new Random();

            // Mersenne Twister algorthm for uniform distribitions
            var random = new MersenneTwister(13);

            // digits of courdinates
            int numDigids = 10000;

            // hash cordinates
            HashSet<string> hashSet = new HashSet<string>();
            string tempLat = "";
            string tempLon = "";
            string tempHig = "";
            string finalHash = "";

            /// Laplace Distribusion with μ = 4000 and β = 100
            int m = 7000;   // location
            int b = 200;    // spread the distrubuion
            var Laplace = new Laplace(m, b);

            for (var i = 0; i < NumberOfDocuments; i++)
            {
                var SyntheticObject = new GlobalUrbanPoint();

                // Change random year
                randomDate = getRandomDate();

                //Take the probaliety for each data point
                propability = randGen.Next(0, 100) < trueChance ? 1 : 0;

                /// Generate Uniform Random Coordinates 
                while (true)
                {
                    if (propability == 1)
                    {
                        SyntheticObject.LATITUDE = (int)(random.NextDecimal() * numDigids);
                        SyntheticObject.LONGITUDE = (int)(random.NextDecimal() * numDigids);
                        SyntheticObject.HIGTH = (int)(random.NextDecimal() * numDigids);
                    }
                    else
                    {
                        SyntheticObject.LATITUDE = (int)Laplace.Sample(m, b);
                        SyntheticObject.LONGITUDE = (int)Laplace.Sample(m, b);
                        SyntheticObject.HIGTH = (int)Laplace.Sample(m, b);
                    }

                    tempLat = SyntheticObject.LATITUDE.ToString();
                    tempLon = SyntheticObject.LONGITUDE.ToString();
                    tempHig = SyntheticObject.HIGTH.ToString();

                    if (tempLat.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";
                        for (int iL = 0; iL < numDigids.ToString().Length - tempLat.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempLat = addZeros + tempLat;
                    }

                    if (tempLon.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";

                        for (int iL = 0; iL < numDigids.ToString().Length - tempLon.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempLon = addZeros + tempLon;
                    }

                    if (tempHig.Length < numDigids.ToString().Length)
                    {
                        string addZeros = "";

                        for (int iL = 0; iL < numDigids.ToString().Length - tempHig.Length; iL++)
                        {
                            addZeros += "0";
                        }

                        tempHig = addZeros + tempHig;
                    }

                    finalHash = tempLat + "," + tempLon + "," + tempHig;

                    if (hashSet.Contains(finalHash) == false)
                    {
                        hashSet.Add(finalHash);
                        break;
                    }
                }

                SyntheticObject.OBJECTID = i;
                SyntheticObject.URBORRUR = RandomString(1);
                SyntheticObject.YEAR_ = randomDate.Year;
                SyntheticObject.ES90POP = (uint)rnd.Next(1000, 10000000);
                SyntheticObject.ES95POP = SyntheticObject.ES90POP + (uint)rnd.Next(20000, 10000000);
                SyntheticObject.ES00POP = SyntheticObject.ES95POP + (uint)(((20.0 / 100.0)) * (double)SyntheticObject.ES95POP);
                SyntheticObject.CONTINENT = PickContinet();
                SyntheticObject.UNREGION = PickUNREGION(SyntheticObject.CONTINENT);
                SyntheticObject.COUNTRY = RandomString(8);
                SyntheticObject.FORGNNM = PickLanguege();
                SyntheticObject.ISO3 = RandomString(3);
                SyntheticObject.UNSD = rnd.Next(1000);
                SyntheticObject.SCHADMNM = RandomString(6);
                SyntheticObject.NAME1 = SyntheticObject.SCHADMNM;
                SyntheticObject.TYPE = PickType();
                SyntheticObject.SCHADMNM = RandomString(8);
                SyntheticObject.ADMNM1 = RandomString(6);
                SyntheticObject.ADMNM2 = SyntheticObject.ADMNM1;
                SyntheticObject.SRCTYP = RandomString(9);
                SyntheticObject.NOTES = RandomString(150);
                SyntheticObject.COORDSRCE = RandomString(4);
                SyntheticObject.LOCNDATSRC = RandomString(40);
                SyntheticObject.DATSRC = RandomString(120);
                SyntheticObject.INSGRUSED = "N";
                SyntheticObject.UNREGION = SyntheticObject.COUNTRY;

                SyntheticList.Add(SyntheticObject);
            }

            Console.WriteLine($"Intemes in list {SyntheticList.Count}");

            //max legth of each dim
            int maxLengthDim = (int)Math.Pow(2.0, (double)bits);

            foreach (var item in SyntheticList)
            {
                item.LATITUDE = ConvertRange(0.0, (double)numDigids, 0, maxLengthDim-1, item.LATITUDE);
                item.LONGITUDE = ConvertRange(0.0, (double)numDigids, 0, maxLengthDim-1, item.LONGITUDE);
                item.HIGTH = ConvertRange(0.0, (double)numDigids, 0, maxLengthDim-1, item.HIGTH);
            }
            return SyntheticList;
        }

        /// <summary>
        /// Convet a number to unother range
        /// </summary>
        /// <param name="originalStart"></param>
        /// <param name="originalEnd"></param>
        /// <param name="newStart"></param>
        /// <param name="newEnd"></param>
        /// <param name="value"></param>
        /// <returns> Transorm a number to a new range</returns>
        public static double ConvertRange(
            double originalStart, double originalEnd, // original range
            int newStart, int newEnd, // desired range
            double value) // value to convert
        {
            double scale = (double)(newEnd - newStart) / (originalEnd - originalStart);
            return (double)(newStart + ((value - originalStart) * scale));
        }

        /// <summary>
        /// The Idea is to feed the fuction with cordinates and return the Snake index 
        ///         
        ///         ---------------------
        ///         | 15 | 14 | 13 | 12 |
        ///         | 8  | 9  | 10 | 11 |
        ///         | 7  | 6  | 5  | 4  |
        ///         | 0  | 1  | 2  | 3  |
        ///         ---------------------
        ///         
        /// </summary>
        /// <param name="bits">Represent the maxim length of each dimension </param>
        /// <param name="x"> Condinates</param>
        /// <param name="y"> Condinates</param>
        /// <returns>The index of snake curve </returns>
        public static ulong SnakeCurveIndex(uint bits, uint X, uint Y)
        {

            ulong index = 0;
            ulong x = (ulong)X;
            ulong y = (ulong)Y;
            //The dimension of the array
            ulong dim = (ulong)Math.Pow(2.0, (double)bits);

            if (y % (ulong)2 == 0)
            {
                index = x + y * dim;
            }
            else
            {
                index = (dim - 1 - x) + y * dim;
            }

            //Console.WriteLine($"The dimesons of array is [{dim},{dim}] \nWith capasity {dim * dim}");
            if ((BigInteger)index >= (BigInteger)((BigInteger)dim * (BigInteger)dim))
            {
                //Debug console 
                throw new Exception("The index is out of bounts");
            }

            return index;
        }

        public static ulong SnakeCurveIndex(uint bits, uint X, uint Y, uint Z)
        {
            ulong index = 0;
            ulong x = (ulong)X;
            ulong y = (ulong)Y;
            ulong z = (ulong)Z;

            // The size of each dimension of the array
            ulong dim = (ulong)Math.Pow(2.0, (double)bits);
            index = z * dim * dim;

            if (z % (ulong)2 == 0)
            {
                if (y % (ulong)2 == 0)
                {
                    index += x + y * dim;
                }
                else
                {
                    index += (dim - 1 - x) + y * dim;
                }
            }
            else
            {
                if (y % (ulong)2 == 0)
                {
                    index += (dim * dim - 1) - y * dim - x;
                }
                else
                {
                    index += (dim * dim) - y * dim - (dim - x);
                }
            }

            if (x > dim || y > dim || z > dim)
            {
                //Debug console 
                throw new Exception("The index is out of bounts");
            }

            return index;
        }

        //Recall and Precision
        /// <summary>
        /// Calculate the precision and recall for the querrys.
        /// Equestion in the fowlloing link chapre 2 page 26-28: https://repository.kallipos.gr/pdfviewer/web/viewer.html?file=/bitstream/11419/4191/2/irbook.pdf
        /// </summary>
        /// <param name="courdQuerry">The corect querry</param>
        /// <param name="anserQuerry">The alternative querry</param>
        /// <returns>Precision,Recall</returns>
        public static (double precision, double recall) PrecisionAndRecall(List<GlobalUrbanPoint> courdQuerry, List<GlobalUrbanPoint> anserQuerry)
        {
            int Ar = 0;
            for (int i = 0; i < anserQuerry.Count(); i++)
            {
                for (int j = 0; j < courdQuerry.Count(); j++)
                {
                    if (anserQuerry[i].OBJECTID == courdQuerry[j].OBJECTID)
                    {
                        Ar++;
                        break;
                    }
                }

            }

            double recall = (double)Ar / (double)courdQuerry.Count();
            double precision = (double)Ar / (double)anserQuerry.Count();

            return (precision, recall);
        }



        //Recall and Precision
        /// <summary>
        /// Calculate the precision and recall for the querrys Thread Version.
        /// Equestion in the fowlloing link chapre 2 page 26-28: https://repository.kallipos.gr/pdfviewer/web/viewer.html?file=/bitstream/11419/4191/2/irbook.pdf
        /// </summary>
        /// <param name="courdQuerry">The corect querry</param>
        /// <param name="anserQuerry">The alternative querry</param>
        /// <returns>Precision,Recall</returns>
        public static (double precision, double recall) PrecisionAndRecallThread(List<GlobalUrbanPoint> courdQuery, List<GlobalUrbanPoint> anserQuery)
        {
            // Calculate the slide of each thread
            SlideCount = new int[NumThredsPreRecal];
            SizeBlock = ((anserQuery.Count + NumThredsPreRecal - 1) / NumThredsPreRecal);
            Console.WriteLine($"SizeBlock = {SizeBlock}");

            int Ar = 0;
            //pass the variables to threads
            AnserQuery = anserQuery;
            CourdQuery = courdQuery;


            //Create a therad array
            Thread[] threads = new Thread[NumThredsPreRecal];
            //Start Threads
            for (int i = 0; i < NumThredsPreRecal; i++)
            {
                threads[i] = new Thread(CountTheSlide);
                threads[i].Start(i);
            }

            // Join the threads ween he finish
            for (int i = 0; i < NumThredsPreRecal; i++)
            {
                threads[i].Join();
            }

            for (int i = 0; i < NumThredsPreRecal; i++)
            {
                Ar += SlideCount[i];
            }

            double recall = (double)Ar / (double)courdQuery.Count();
            double precision = (double)Ar / (double)anserQuery.Count();

            return (precision, recall);
        }

        /// <summary>
        /// Count the current slide of array in each threads
        /// </summary>
        /// <param name="id"></param>
        public static void CountTheSlide(object id)
        {
            int count = 0;
            int start = (int)id;//id of thread as int
            int baseIndex = start * SizeBlock;
            if ((int)id != NumThredsPreRecal - 1)
            {
                for (int i = baseIndex; i < baseIndex + SizeBlock; i++)
                {
                    for (int j = 0; j < CourdQuery.Count(); j++)
                    {
                        if (AnserQuery[i].OBJECTID == CourdQuery[j].OBJECTID)
                        {
                            count++;
                            break;
                        }
                    }
                }
            }
            else // The last thread has less elements
            {
                int AntileTheEnd = AnserQuery.Count();
                for (int i = (NumThredsPreRecal - 1) * SizeBlock; i < AntileTheEnd; i++)
                {
                    for (int j = 0; j < CourdQuery.Count(); j++)
                    {
                        if (AnserQuery[i].OBJECTID == CourdQuery[j].OBJECTID)
                        {
                            count++;
                            break;
                        }
                    }
                }

            }

            SlideCount[start] = count;
        }

        // CSV format class 
        public class FormatCsv
        {
            public double TimeAddHilberd2d { get; set; }
            public double TimeDifindedHilberd2d { get; set; }
            public double TimeAddSnake2d { get; set; }
            public double TimeDifinedSnake2d { get; set; }
            public double TimeAddZOrder2d { get; set; }
            public double TimeDifinedZOrder2d { get; set; }

            public double TimeQueryCourdinates2d1Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates2d1Percent { get; set; }
            public double TimeQueryHilber2d1Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert2d1Percent { get; set; }
            public double RecallHilbert2d1Percent { get; set; }
            public double PrecisionHilbert2d1Percent { get; set; }
            public double TimeQuerySnake2d1Percent { get; set; }
            public int DocumetsRetreveQuerrySnake2d1Percent { get; set; }
            public double RecallSnake2d1Percent { get; set; }
            public double PrecisionSbake2d1Percent { get; set; }
            public double TimeQueryZorder2d1Percent { get; set; }
            public int DocumetsRetreveQuerryZorder2d1Percent { get; set; }
            public double RecallZorder2d1Percent { get; set; }
            public double PrecisionZorder2d1Percent { get; set; }

            public double TimeQueryCourdinates2d5Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates2d5Percent { get; set; }
            public double TimeQueryHilber2d5Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert2d5Percent { get; set; }
            public double RecallHilbert2d5Percent { get; set; }
            public double PrecisionHilbert2d5Percent { get; set; }
            public double TimeQuerySnake2d5Percent { get; set; }
            public int DocumetsRetreveQuerrySnake2d5Percent { get; set; }
            public double RecallSnake2d5Percent { get; set; }
            public double PrecisionSbake2d5Percent { get; set; }
            public double TimeQueryZorder2d5Percent { get; set; }
            public int DocumetsRetreveQuerryZorder2d5Percent { get; set; }
            public double RecallZorder2d5Percent { get; set; }
            public double PrecisionZorder2d5Percent { get; set; }

            public double TimeQueryCourdinates2d20Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates2d20Percent { get; set; }
            public double TimeQueryHilber2d20Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert2d20Percent { get; set; }
            public double RecallHilbert2d20Percent { get; set; }
            public double PrecisionHilbert2d20Percent { get; set; }
            public double TimeQuerySnake2d20Percent { get; set; }
            public int DocumetsRetreveQuerrySnake2d20Percent { get; set; }
            public double RecallSnake2d20Percent { get; set; }
            public double PrecisionSbake2d20Percent { get; set; }
            public double TimeQueryZorder2d20Percent { get; set; }
            public int DocumetsRetreveQuerryZorder2d20Percent { get; set; }
            public double RecallZorder2d20Percent { get; set; }
            public double PrecisionZorder2d20Percent { get; set; }

            public double TimeAddHilberd3d { get; set; }
            public double TimeDifindedHilberd3d { get; set; }
            public double TimeAddSnake3d { get; set; }
            public double TimeDifinedSnake3d { get; set; }
            public double TimeAddZOrder3d { get; set; }
            public double TimeDifinedZOrder3d { get; set; }

            public double TimeQueryCourdinates3d1Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates3d1Percent { get; set; }
            public double TimeQueryHilber3d1Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert3d1Percent { get; set; }
            public double RecallHilbert3d1Percent { get; set; }
            public double PrecisionHilbert3d1Percent { get; set; }
            public double TimeQuerySnake3d1Percent { get; set; }
            public int DocumetsRetreveQuerrySnake3d1Percent { get; set; }
            public double RecallSnake3d1Percent { get; set; }
            public double PrecisionSbake3d1Percent { get; set; }
            public double TimeQueryZorder3d1Percent { get; set; }
            public int DocumetsRetreveQuerryZorder3d1Percent { get; set; }
            public double RecallZorder3d1Percent { get; set; }
            public double PrecisionZorder3d1Percent { get; set; }

            public double TimeQueryCourdinates3d5Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates3d5Percent { get; set; }
            public double TimeQueryHilber3d5Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert3d5Percent { get; set; }
            public double RecallHilbert3d5Percent { get; set; }
            public double PrecisionHilbert3d5Percent { get; set; }
            public double TimeQuerySnake3d5Percent { get; set; }
            public int DocumetsRetreveQuerrySnake3d5Percent { get; set; }
            public double RecallSnake3d5Percent { get; set; }
            public double PrecisionSbake3d5Percent { get; set; }
            public double TimeQueryZorder3d5Percent { get; set; }
            public int DocumetsRetreveQuerryZorder3d5Percent { get; set; }
            public double RecallZorder3d5Percent { get; set; }
            public double PrecisionZorder3d5Percent { get; set; }

            public double TimeQueryCourdinates3d20Percent { get; set; }
            public int DocumetsRetreveQuerryCourdinates3d20Percent { get; set; }
            public double TimeQueryHilber3d20Percent { get; set; }
            public int DocumetsRetreveQuerryHilbert3d20Percent { get; set; }
            public double RecallHilbert3d20Percent { get; set; }
            public double PrecisionHilbert3d20Percent { get; set; }
            public double TimeQuerySnake3d20Percent { get; set; }
            public int DocumetsRetreveQuerrySnake3d20Percent { get; set; }
            public double RecallSnake3d20Percent { get; set; }
            public double PrecisionSbake3d20Percent { get; set; }
            public double TimeQueryZorder3d20Percent { get; set; }
            public int DocumetsRetreveQuerryZorder3d20Percent { get; set; }
            public double RecallZorder3d20Percent { get; set; }
            public double PrecisionZorder3d20Percent { get; set; }
        }

        // The class of my model
        public class GlobalUrbanPoint
        {
            [BsonId]
            public ObjectId Id { get; set; }
            public int OBJECTID { get; set; }
            public int LATLONGID { get; set; }
            public double LATITUDE { get; set; } // Axis of X
            public double LONGITUDE { get; set; } // Axis of Y
            public double HIGTH { get; set; } // Axis of Z
            public string URBORRUR { get; set; }
            public int YEAR_ { get; set; }
            public uint ES90POP { get; set; }
            public uint ES95POP { get; set; }
            public uint ES00POP { get; set; }
            public string INSGRUSED { get; set; }
            public string CONTINENT { get; set; }
            public string UNREGION { get; set; }
            public string COUNTRY { get; set; }
            public float UNSD { get; set; }
            public string ISO3 { get; set; }
            public string SCHNM { get; set; }
            public string NAME1 { get; set; }
            public string NAME2 { get; set; }
            public string NAME3 { get; set; }
            public string FORGNNM { get; set; }
            public string SCHADMNM { get; set; }
            public string ADMNM1 { get; set; }
            public string ADMNM2 { get; set; }
            public string TYPE { get; set; }
            public string SRCTYP { get; set; }
            public string COORDSRCE { get; set; }
            public string DATSRC { get; set; }
            public string LOCNDATSRC { get; set; }
            public string NOTES { get; set; }
            public ulong HilbertIndex { get; set; }
            public ulong SnakeIndex { get; set; }
            public ulong ZOrderIndex { get; set; }

            public GlobalUrbanPoint()
            {
                this.Id = ObjectId.GenerateNewId();
            }
        }

        //MonogCRUD Operations
        public class MongoCRUD
        {
            private IMongoDatabase db;

            public MongoCRUD(string database)
            {
                var client = new MongoClient();
                db = client.GetDatabase(database);
            }

            // Insert Records
            public void InsertRecord<T>(string table, T record)
            {
                var collection = db.GetCollection<T>(table);
                collection.InsertOne(record);
            }

            public void InsertMultipleRecords<T>(string table, List<T> records)
            {
                var collection = db.GetCollection<T>(table);
                collection.InsertMany(records);
            }

            // Load Reacords
            public List<T> LoadRecords<T>(string table)
            {
                var collection = db.GetCollection<T>(table);
                return collection.Find(new BsonDocument()).ToList();
            }

            public T LoadRecordByCountry<T>(string table, string country)
            {
                var collection = db.GetCollection<T>(table);
                var filter = Builders<T>.Filter.Eq("COUNTRY", country);
                return collection.Find(filter).First();
            }

            // Copy the database
            public void CopyColection<T>(string targetTable, string newTable)
            {
                var source_colection = db.GetCollection<T>(targetTable);
                var dest = db.GetCollection<T>(newTable);
                dest.InsertMany(source_colection.Find(new BsonDocument()).ToList());

            }

            // Normalize coordinates to [0,1]
            public void NormalizeCoordinates<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                // Take the min and max values from a spesifics filds of the colection
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Min(f => f.LONGITUDE) });
                var minLogitude = query.First().MinVal;

                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });
                var maxLogitude = query.First().MinVal;

                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Min(f => f.LATITUDE) });
                var minLatitude = query.First().MinVal;

                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });
                var maxLatitude = query.First().MinVal;

                // calulate the diagonal of the square
                var length_of_diagonal = Math.Sqrt(Math.Pow(maxLatitude - minLatitude, 2) + Math.Pow(maxLogitude - minLogitude, 2));


                Console.WriteLine("Min Longitude :" + minLogitude.ToString());
                Console.WriteLine("Max Longitude :" + maxLogitude.ToString());
                Console.WriteLine("Min Latitude :" + minLatitude.ToString());
                Console.WriteLine("Max Latitude :" + maxLatitude.ToString());
                Console.WriteLine("Diagonal of square :" + length_of_diagonal.ToString() + "\n -----------------------\n\n");

                // load the records for up date 
                //var records = collection.Find(new BsonDocument()).ToList();
                var records = collection.Find<T>(x => true).ToList();

                var normalize_Latitude = 0.0;
                var normalize_Longitude = 0.0;

                foreach (var rec in records)
                {

                    // Match  with the curent record 
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    normalize_Latitude = (rec.LATITUDE - minLatitude) / (length_of_diagonal);
                    normalize_Longitude = (rec.LONGITUDE - minLogitude) / (length_of_diagonal);

                    //Console.WriteLine("Record from console :" + rec.Id.ToString() + "Norm lat:" + normalize_Latitude.ToString());
                    //Console.WriteLine("Record from console :" + rec.Id.ToString() + "Norm lon:" + normalize_Longitude.ToString());

                    // udatate the entry of the record
                    var update = Builders<T>.Update.Set("LATITUDE", normalize_Latitude).Set("LONGITUDE", normalize_Longitude);

                    collection.UpdateOne(filter, update);
                    //Console.WriteLine("Record from console :" + rec.Id.ToString());
                }
            }

            // 2D and 3D function for documnts index

            public void AddHilbertIndex2D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));

                // load all records to a list
                var records = collection.Find(new BsonDocument()).ToList();

                // Bits for Hilbert point
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates

                //Console.WriteLine("New Range of [" + newMinRange.ToString() + ',' + newMaxRange.ToString() +']');
                //Console.WriteLine("Range of Hilber in each Diamension {0:D}", bits);
                foreach (var rec in records)
                {
                    // Match  with the curent record 
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    // get the cordinates
                    double latudude = Math.Round(rec.LATITUDE);    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    // change the range of cordinates if it is needed
                    double x_Cordinate = latudude;//ConvertRange(-90.0, 90.0, 0, 10, latudude);
                    double y_Cordinate = longitude;//ConvertRange(-180.0, 180.0, 0, 10, longitude);

                    uint[] Cordinates = { (uint)Math.Round(x_Cordinate), (uint)Math.Round(y_Cordinate) };

                    // Hilbert point
                    var hPoint = new HilbertPoint(Cordinates, bits);
                    BigInteger index2 = hPoint.HilbertIndex;

                    // Debuege console
                    //Console.WriteLine("Codrdinates Raw [Latadude,logitude] = [{0:f},{1:f}]", latudude,longitude);
                    //Console.WriteLine("Codrdinates new [Latadude,logitude] = [{0:f},{1:f}]", Math.Round(x_Cordinate), Math.Round(y_Cordinate));
                    //Console.WriteLine("Hilbert index {0:D}", index2);

                    //Set hilbet index to the record
                    var update = Builders<T>.Update.Set("HilbertIndex", (uint)index2);
                    collection.UpdateOne(filter, update);
                }
                Console.WriteLine("Finish 2d Hilbet");
            }

            public void AddHilbertIndex3D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.HIGTH) });//maxHigth
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));

                // load all records to a list
                var records = collection.Find(new BsonDocument()).ToList();

                // Bits for Hilbert point
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates


                //Console.WriteLine("New Range of [" + newMinRange.ToString() + ',' + newMaxRange.ToString() +']');
                //Console.WriteLine("Range of Hilber in each Diamension {0:D}", bits);
                foreach (var rec in records)
                {
                    // Match  with the curent record 
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;          // axis Z

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    uint[] Cordinates = { (uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate };

                    // Hilbert point
                    var hPoint = new HilbertPoint(Cordinates, bits);
                    BigInteger index = hPoint.HilbertIndex;

                    // Debuege console
                    //Console.WriteLine("Codrdinates Raw [Latadude,logitude] = [{0:f},{1:f}]", latudude,longitude);
                    //Console.WriteLine("Codrdinates new [Latadude,logitude] = [{0:f},{1:f}]", Math.Round(x_Cordinate), Math.Round(y_Cordinate));
                    //Console.WriteLine("Hilbert index {0:D}", index2);

                    //Set hilbet index to the recordes
                    var update = Builders<T>.Update.Set("HilbertIndex", (uint)index);
                    collection.UpdateOne(filter, update);

                }
                Console.WriteLine("Finish 3d Hilbet");
            }

            public void AddSnakeIndex2D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);

                    ulong SnakeIndex = SnakeCurveIndex((uint)bits, (uint)x_Cordinate, (uint)y_Cordinate);

                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    var update = Builders<T>.Update.Set("SnakeIndex", SnakeIndex);
                    collection.UpdateOne(filter, update);
                }
                Console.WriteLine("Finish 2d Snake");
            }

            public void AddSnakeIndex3D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.HIGTH) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));

                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    ulong SnakeIndex3D = SnakeCurveIndex((uint)bits, (uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate);

                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    var update = Builders<T>.Update.Set("SnakeIndex", SnakeIndex3D);
                    collection.UpdateOne(filter, update);
                }
                Console.WriteLine("Finish 3d Snake");
            }

            public void AddZOrderIndex2D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);

                    // z curve
                    var z_index = MortonEncoding.Encode((uint)x_Cordinate, (uint)y_Cordinate);
                    //find the document
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);
                    //update the document
                    var update = Builders<T>.Update.Set("ZOrderIndex", z_index);
                    collection.UpdateOne(filter, update);
                }
                Console.WriteLine("Finish 2d Zorder");
            }

            public void AddZOrderIndex3D<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    // z curve
                    var z_index = (ulong)Tedd.MortonEncoding.Encode((uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate);
                    //find the document
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);
                    //update the document
                    var update = Builders<T>.Update.Set("ZOrderIndex", z_index);
                    collection.UpdateOne(filter, update);
                }
                Console.WriteLine("Finish 3d Zorder");
            }

            // Difine index
            public void DefineHilbertIndex<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.HilbertIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
            }

            public void DefineSnakeIndex<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.SnakeIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
            }

            public void DefineZOrderIndex<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.ZOrderIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
            }


            // Thread Versions 2d Indexes
            public double AddHilbertIndex2DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);

                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));

                // load all records to a list
                var records = collection.Find(new BsonDocument()).ToList();

                // Bits for Hilbert point
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates

                //Console.WriteLine("New Range of [" + newMinRange.ToString() + ',' + newMaxRange.ToString() +']');
                //Console.WriteLine("Range of Hilber in each Diamension {0:D}", bits);
                foreach (var rec in records)
                {
                    // Match  with the curent record 
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    // get the cordinates
                    double latudude = Math.Round(rec.LATITUDE);    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    // change the range of cordinates if it is needed
                    double x_Cordinate = latudude;//ConvertRange(-90.0, 90.0, 0, 10, latudude);
                    double y_Cordinate = longitude;//ConvertRange(-180.0, 180.0, 0, 10, longitude);

                    uint[] Cordinates = { (uint)Math.Round(x_Cordinate), (uint)Math.Round(y_Cordinate) };

                    // Hilbert point
                    var hPoint = new HilbertPoint(Cordinates, bits);
                    BigInteger index2 = hPoint.HilbertIndex;

                    // Debuege console
                    //Console.WriteLine("Codrdinates Raw [Latadude,logitude] = [{0:f},{1:f}]", latudude,longitude);
                    //Console.WriteLine("Codrdinates new [Latadude,logitude] = [{0:f},{1:f}]", Math.Round(x_Cordinate), Math.Round(y_Cordinate));
                    //Console.WriteLine("Hilbert index {0:D}", index2);

                    //Set hilbet index to the recordes
                    var update = Builders<T>.Update.Set("HilbertIndex", (ulong)index2);
                    collection.UpdateOne(filter, update);
                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                Console.WriteLine("Finish 2d Hilbet");
                return time;
            }

            public double AddSnakeIndex2DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);

                    ulong SnakeIndex = SnakeCurveIndex((uint)bits, (uint)x_Cordinate, (uint)y_Cordinate);

                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    var update = Builders<T>.Update.Set("SnakeIndex", SnakeIndex);
                    collection.UpdateOne(filter, update);
                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                Console.WriteLine("Finish 2d Snake");
                return time;
            }

            public double AddZOrderIndex2DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);

                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y

                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);

                    // z curve
                    var z_index = Tedd.MortonEncoding.Encode((uint)x_Cordinate, (uint)y_Cordinate);
                    //find the document
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);
                    //update the document
                    var update = Builders<T>.Update.Set("ZOrderIndex", z_index);
                    collection.UpdateOne(filter, update);
                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                Console.WriteLine("Finish 2d Zorder");
                return time;
            }

            // Therad Version 3d Indexes
            public double AddHilbertIndex3DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);

                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.HIGTH) });//maxHigth
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));

                // load all records to a list
                var records = collection.Find(new BsonDocument()).ToList();

                // Bits for Hilbert point
                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates


                //Console.WriteLine("New Range of [" + newMinRange.ToString() + ',' + newMaxRange.ToString() +']');
                //Console.WriteLine("Range of Hilber in each Diamension {0:D}", bits);
                foreach (var rec in records)
                {
                    // Match  with the curent record 
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;          // axis Z

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    uint[] Cordinates = { (uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate };

                    // Hilbert point
                    var hPoint = new HilbertPoint(Cordinates, bits);
                    BigInteger index = hPoint.HilbertIndex;

                    // Debuege console
                    //Console.WriteLine("Codrdinates Raw [Latadude,logitude] = [{0:f},{1:f}]", latudude,longitude);
                    //Console.WriteLine("Codrdinates new [Latadude,logitude] = [{0:f},{1:f}]", Math.Round(x_Cordinate), Math.Round(y_Cordinate));
                    //Console.WriteLine("Hilbert index {0:D}", index2);

                    //Set hilbet index to the recordes
                    var update = Builders<T>.Update.Set("HilbertIndex", (ulong)index);
                    collection.UpdateOne(filter, update);

                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                Console.WriteLine("Finish 3d Hilbet");
                return time;

            }

            public double AddSnakeIndex3DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var ListMaxCordinates = new List<double> { };
                var query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LONGITUDE) });//maxLogitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));
                query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.HIGTH) });//maxLatitude
                ListMaxCordinates.Add(Math.Round(query.First().MinVal));


                int bits = HilbertTransformation.FastHilbert.SmallestPowerOfTwo((int)ListMaxCordinates.Max()); //The smaleste power of two of the max cordinates

                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    // get the cordinates
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    ulong SnakeIndex3D = SnakeCurveIndex((uint)bits, (uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate);

                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);

                    var update = Builders<T>.Update.Set("SnakeIndex", SnakeIndex3D);
                    collection.UpdateOne(filter, update);
                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                Console.WriteLine("Finish 3d Snake");
                return time;
            }

            public double AddZOrderIndex3DThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var records = collection.Find<T>(x => true).ToList();

                foreach (var rec in records)
                {
                    double latudude = rec.LATITUDE;    // axis X
                    double longitude = rec.LONGITUDE;  // axis Y
                    double higth = rec.HIGTH;

                    //Round the cordinates
                    double x_Cordinate = Math.Round(latudude);
                    double y_Cordinate = Math.Round(longitude);
                    double z_Cordinate = Math.Round(higth);

                    // z curve
                    var z_index = Tedd.MortonEncoding.Encode((uint)x_Cordinate, (uint)y_Cordinate, (uint)z_Cordinate);
                    //find the document
                    var filter = Builders<T>.Filter.Eq("Id", rec.Id);
                    //update the document
                    var update = Builders<T>.Update.Set("ZOrderIndex", z_index);
                    collection.UpdateOne(filter, update);
                }
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                return time;
            }


            // Difine indexes Thread Versions
            public double DefineHilbertIndexThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.HilbertIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                return time;
            }

            public double DefineSnakeIndexThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.SnakeIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                return time;
            }

            public double DefineZOrderIndexThread<T>(string table) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var keys = Builders<T>.IndexKeys.Ascending(x => x.ZOrderIndex);
                collection.Indexes.CreateOne(new CreateIndexModel<T>(keys, new CreateIndexOptions { Unique = false }));
                watch.Stop();
                double time = watch.ElapsedMilliseconds;
                return time;
            }


            //Delete index
            public void DropAllIndexes<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                collection.Indexes.DropAll();
            }

            //Reset Indexes to 0
            public void ResetIndexVariables<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var filter = Builders<T>.Filter.Empty;
                var update = Builders<T>.Update.Set("HilbertIndex", 0).Set("SnakeIndex", 0).Set("ZOrderIndex", 0);
                collection.UpdateMany(filter, update);
            }

            // Find all difrent values of Unregion Fild
            public List<string> FindAllDistictUnregion<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var filter = new BsonDocument();
                var results = collection.Distinct(x => x.UNREGION, filter);
                return results.ToList();
            }

            //Range Query function for Hilbert,Snake,Zorder for 2 and 3d indexes
            public List<T> RangeQueryHilbert<T>(string table, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY, int bits) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                uint[] CordinatesStarts = { (uint)lowerLeftX, (uint)lowerLeftY };
                uint[] CordinatesEnd = { (uint)upperRightX, (uint)upperRightY };

                var hPointStrat = new HilbertPoint(CordinatesStarts, bits);
                BigInteger indexStart = hPointStrat.HilbertIndex;

                var hPointEnd = new HilbertPoint(CordinatesEnd, bits);
                BigInteger indexEnd = hPointEnd.HilbertIndex;

                //just for the inisialization of filter
                var filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexEnd);

                // update the filter
                if ((ulong)indexEnd > (ulong)indexStart)
                {
                    filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexEnd);
                }
                else
                {
                    filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexEnd) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexStart);
                }    
                return collection.Find(filter).ToList();
            }

            public List<T> RangeQueryHilbert<T>(string table, double lowerLeftX, double lowerLeftY, double lowerLeftZ, double upperRightX, double upperRightY, double upperRightZ, int bits) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                uint[] CordinatesStarts = { (uint)lowerLeftX, (uint)lowerLeftY, (uint)lowerLeftZ };
                uint[] CordinatesEnd = { (uint)upperRightX, (uint)upperRightY, (uint)upperRightZ };

                var hPointStrat = new HilbertPoint(CordinatesStarts, bits);
                BigInteger indexStart = hPointStrat.HilbertIndex;

                var hPointEnd = new HilbertPoint(CordinatesEnd, bits);
                BigInteger indexEnd = hPointEnd.HilbertIndex;

                var filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexEnd);

                // update the filter
                if ((ulong)indexEnd > (ulong)indexStart)
                {
                    filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexEnd);
                }
                else
                {
                    filter = Builders<T>.Filter.Gte(x => x.HilbertIndex, (ulong)indexEnd) & Builders<T>.Filter.Lte(x => x.HilbertIndex, (ulong)indexStart);
                }

                return collection.Find(filter).ToList();
            }

            public List<T> RangeQuerySnake<T>(string table, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY, int bits) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var indexStart = SnakeCurveIndex((uint)bits, (uint)lowerLeftX, (uint)lowerLeftY);
                var indexEnd = SnakeCurveIndex((uint)bits, (uint)upperRightX, (uint)upperRightY);

                var filter = Builders<T>.Filter.Gte(x => x.SnakeIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.SnakeIndex, (ulong)indexEnd);
                return collection.Find(filter).ToList();
            }

            public List<T> RangeQuerySnake<T>(string table, double lowerLeftX, double lowerLeftY, double lowerLeftZ, double upperRightX, double upperRightY, double upperRightZ, int bits) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var indexStart = SnakeCurveIndex((uint)bits, (uint)lowerLeftX, (uint)lowerLeftY, (uint)lowerLeftZ);
                var indexEnd = SnakeCurveIndex((uint)bits, (uint)upperRightX, (uint)upperRightY, (uint)upperRightZ);

                var filter = Builders<T>.Filter.Gte(x => x.SnakeIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.SnakeIndex, (ulong)indexEnd);
                return collection.Find(filter).ToList();
            }

            public List<T> RangeQueryZOreder<T>(string table, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var indexStart = Tedd.MortonEncoding.Encode((uint)lowerLeftX, (uint)lowerLeftY);
                var indexEnd = Tedd.MortonEncoding.Encode((uint)upperRightX, (uint)upperRightY);

                var filter = Builders<T>.Filter.Gte(x => x.ZOrderIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.ZOrderIndex, (ulong)indexEnd);
                return collection.Find(filter).ToList();
            }

            public List<T> RangeQueryZOreder<T>(string table, double lowerLeftX, double lowerLeftY, double lowerLeftZ, double upperRightX, double upperRightY, double upperRightZ) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);

                var indexStart = Tedd.MortonEncoding.Encode((uint)lowerLeftX, (uint)lowerLeftY, (uint)lowerLeftZ);
                var indexEnd = Tedd.MortonEncoding.Encode((uint)upperRightX, (uint)upperRightY, (uint)upperRightZ);

                var filter = Builders<T>.Filter.Gte(x => x.ZOrderIndex, (ulong)indexStart) & Builders<T>.Filter.Lte(x => x.ZOrderIndex, (ulong)indexEnd);
                return collection.Find(filter).ToList();
            }

            public List<T> RangeQueryCodrinates<T>(string table, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var filterLat = Builders<T>.Filter.Gte(x => x.LATITUDE, lowerLeftX) & Builders<T>.Filter.Lte(x => x.LATITUDE, upperRightX);
                var filterLon = Builders<T>.Filter.Gte(y => y.LONGITUDE, lowerLeftY) & Builders<T>.Filter.Lte(y => y.LONGITUDE, upperRightY);
                var cordinatesFilter = filterLat & filterLon;
                return collection.Find<T>(cordinatesFilter).ToList();
            }

            public List<T> RangeQueryCodrinates<T>(string table, double lowerLeftX, double lowerLeftY, double lowerLeftZ, double upperRightX, double upperRightY, double upperRightZ) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var filterLat = Builders<T>.Filter.Gte(x => x.LATITUDE, lowerLeftX) & Builders<T>.Filter.Lte(x => x.LATITUDE, upperRightX);
                var filterLon = Builders<T>.Filter.Gte(y => y.LONGITUDE, lowerLeftY) & Builders<T>.Filter.Lte(y => y.LONGITUDE, upperRightY);
                var filterHig = Builders<T>.Filter.Gte(z => z.HIGTH, lowerLeftZ) & Builders<T>.Filter.Lte(z => z.HIGTH, upperRightZ);
                var cordinatesFilter = filterLat & filterLon & filterHig;
                return collection.Find<T>(cordinatesFilter).ToList();
            }

            public (List<T>,double) RangeQueryCodrinates2<T>(string table, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY) where T : GlobalUrbanPoint
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var collection = db.GetCollection<T>(table);
                var filterLat = Builders<T>.Filter.Gte(x => x.LATITUDE, lowerLeftX) & Builders<T>.Filter.Lte(x => x.LATITUDE, upperRightX);
                var filterLon = Builders<T>.Filter.Gte(y => y.LONGITUDE, lowerLeftY) & Builders<T>.Filter.Lte(y => y.LONGITUDE, upperRightY);
                var cordinatesFilter = filterLat & filterLon;
                watch.Stop();
                double time = (double)watch.ElapsedMilliseconds;
                return (collection.Find<T>(cordinatesFilter).ToList(),time);
            }

            // Unused Functions
            public void FindByHilbertIndex<T>(string table, uint Hilbert) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                var filter = Builders<T>.Filter.Eq("HilbertIndex", Hilbert);
                var results = collection.Find(filter).ToList();

                foreach (var item in results)
                {
                    Console.WriteLine($"LATLONGID: {item.LATLONGID}");
                    Console.WriteLine($"Coutry: {item.COUNTRY}");
                    Console.WriteLine($"Latidude: {item.LATITUDE}");
                    Console.WriteLine($"Longitude: {item.LONGITUDE}");
                    Console.WriteLine($"Hilbert: {item.HilbertIndex}");
                    Console.WriteLine("=================\n\n");
                }

            }

            // Very Time consuming for large collection maybe it is exist more efficient way to do that. But i can't find it.
            // Don't use it
            public void HandleDouplicateValuesOnHilbertIndex<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                // query = collection.Aggregate().Group(x => 1, gr => new { MinVal = gr.Max(f => f.LATITUDE) });
                // Na to Tsekaro pali an einai sosto to erotima
                var query = collection.Aggregate().Group(point => point.HilbertIndex,
                                                         records => new { Id = records.Key, Count = records.Count() })
                                                  .Match(x => x.Count > 1)
                                                  .Project(x => new { HilbertIndex = x.Id })
                                                  .ToList();

                Console.WriteLine($"Records who have duplicate hilbert index: {query.Count}");


                foreach (var duplicate_value_item in query)
                {
                    //Console.WriteLine($"Record: {i}");
                    //Console.WriteLine($"Duplicate HilbertValu: {duplicate_value_item.HilbertIndex}");

                    // Find the duplicates by the valaue of Hilbet index
                    var filter_of_duplicate = Builders<T>.Filter.Eq("HilbertIndex", duplicate_value_item.HilbertIndex);

                    // Take the fisrt result
                    var results_of_Hilbert = collection.Find(filter_of_duplicate).First();

                    //var results_of_Hilbert_all = collection.Find(filter_of_duplicate).ToList();

                    // Find all records how has defrent Id values from Tje First Result
                    var filter_of_not_equal = Builders<T>.Filter.Ne("Id", results_of_Hilbert.Id);

                    // Condbine the filters with logical And
                    var filters = filter_of_duplicate & filter_of_not_equal;

                    //For debug retern me the resylts from the filters 
                    /*
                    var results = collection.Find(filters).ToList();
                    foreach(var rec in results)
                    {
                        Console.WriteLine($" Id of object = {rec.Id} \n Hilbert index {rec.HilbertIndex}");
                    }
                    */

                    collection.DeleteMany(filters);
                    // i++;
                }


            }
            public void HandleDuplicateValuesOnSnakeIndex<T>(string table) where T : GlobalUrbanPoint
            {
                var collection = db.GetCollection<T>(table);
                //Find duplicates values
                var query = collection.Aggregate().Group(point => point.SnakeIndex,
                                         records => new { Id = records.Key, Count = records.Count() })
                                  .Match(x => x.Count > 1)
                                  .Project(x => new { SnakeIndex = x.Id })
                                  .ToList();


                foreach (var duplicate_value_item in query)
                {
                    // Find the duplicates by the valaue of Snake index
                    var filter_of_duplicate = Builders<T>.Filter.Eq("SnakeIndex", duplicate_value_item.SnakeIndex);

                    // Take the fisrt result
                    var results_of_Snake = collection.Find(filter_of_duplicate).First();

                    // Find all records how has defrent Id values from Tje First Result
                    var filter_of_not_equal = Builders<T>.Filter.Ne("Id", results_of_Snake.Id);

                    // Condbine the filters with logical And
                    var filters = filter_of_duplicate & filter_of_not_equal;

                    collection.DeleteMany(filters);

                }
            }

            //To Do
            //1)https://sigmodrecord.org/publications/sigmodRecord/0103/3.lawder.pdf
            //2)https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.37.3138&rep=rep1&type=pdf
        }
    }
}