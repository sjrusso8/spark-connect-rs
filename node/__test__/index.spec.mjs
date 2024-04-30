import test from 'ava'

import { SparkSession, DataFrame } from '../index.js'

test('spark session', async (t) => {
  let spark = await SparkSession.build("sc://127.0.0.1:15002/");

  console.log(spark);

  const df = await spark.sql("select 'apple' as word, 123 as count");

  await df.show(10);

  t.is(1,1)
})

test("dataframe select/filter/count", async (t) => {
  let spark = await SparkSession.build("sc://127.0.0.1:15002/");

  console.log(spark);

  let df = await spark.sql("select 'apple' as word, 123 as count UNION select 'orange', 456");

  let rows = await df.count();

  t.is(rows,2)

  df = df.select(["word"]).filter("len(word) > 5");

  rows = await df.count();

  await df.show(10);

  t.is(rows,1)
})
