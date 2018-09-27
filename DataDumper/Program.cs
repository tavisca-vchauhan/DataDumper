using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aerospike;
using Aerospike.Client;
using System.IO;
using System.Data;
using CsvHelper;
 
namespace DataDumper
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamReader sr = new StreamReader(@"C:\Users\Vchauhan\Desktop\tweets2.csv");
            CsvReader cr = new CsvReader(sr);
            IEnumerable<Fields> record = cr.GetRecords<Fields>();
            var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
            string nameSpace = "AirEngine";
            string setName = "Vishwas";
            int flag = 0;
            int count = 0;
            foreach (var row in record) 
            {
                if (count <= 20000)
                {
                    if (flag != 0)
                    {
                        
                        var key = new Key(nameSpace, setName, row.tweet_id);
                        aerospikeClient.Put(new WritePolicy(), key, new Bin[] {
                            new Bin("author", row.author),
                            new Bin("content",row.content),
                            new Bin("region",row.region),
                            new Bin("language",row.language),
                            new Bin("following",row.following),
                            new Bin("followers",row.followers),
                            new Bin("tweet_date",row.tweet_date),
                            new Bin("post_type",row.post_type),
                            new Bin("author_id",row.author_id),
                            new Bin("post_url",row.post_url),
                            new Bin("retweet",row.retweet),
                            new Bin("tweet_id",row.tweet_id),
                            
                        });
                    }
                    else
                    {
                        flag = 1;
                    }
                }
            }
            sr.Close();
        }
    }
}