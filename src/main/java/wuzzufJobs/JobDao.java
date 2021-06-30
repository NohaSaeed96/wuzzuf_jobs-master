package wuzzufJobs;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.awt.Color;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;



public class JobDao {
    private static final String COMMA_DELIMITER = ",";
    // CREATE SPARK CONTEXT
    SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext (conf);
    JavaRDD<String> jobs;
    public void read_file() {

        // LOAD DATASETS
         jobs = sparkContext.textFile ("src/main/resources/Wuzzuf_Jobs.csv");

        // TRANSFORMATIONS
        JavaRDD<String> company = jobs
                .map (JobDao::extractCompany);
        // JavaRDD<String>
        JavaRDD<String> words = company.flatMap (tag -> Arrays.asList (tag
                .toLowerCase ()
                .trim ()
                .split ("\\|")).iterator ());
        // JavaRDD<String>
        System.out.println(words.toString ());
        System.out.println(company.count());
    }
    public void clean_data(){

        JavaRDD<String> updated_jobs=jobs.distinct();
        //after removing duplicates we remove the nulls
        JavaRDD<String> company = updated_jobs
                .map (JobDao::extractCompany).filter (StringUtils::isNotBlank);
        System.out.println(company.count());

    }
    public void company_job_count(){
        // COUNTING
        JavaRDD<String> updated_jobs=jobs.distinct();
        //after removing duplicates we remove the nulls
        JavaRDD<String> company = updated_jobs
                .map (JobDao::extractCompany).filter (StringUtils::isNotBlank);

        Map<String, Long> companycounts = company.countByValue ();
        Map<String, Long> final_result = companycounts.entrySet()
                .stream()
               .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
               .collect(Collectors.toMap(
                       Map.Entry::getKey,
                       Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        final_result.forEach((k, v) -> System.out.println("key=" + k + ", value=" + v));

    }

    public static String extractCompany(String company) {
        try {
            return company.split (COMMA_DELIMITER)[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "" ;
        }
    }
    public void pie_chart(Map final_result){
        PieChart chart = new PieChartBuilder ().width (800).height (600).title (getClass ().getSimpleName ()).build ();
        Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
        chart.getStyler ().setSeriesColors (sliceColors);
        chart.addSeries ((String) final_result.keySet().toArray()[0], (Number) final_result.get (final_result.keySet().toArray()[0]));
        chart.addSeries ((String) final_result.keySet().toArray()[1], (Number) final_result.get (final_result.keySet().toArray()[1]));
        chart.addSeries ((String) final_result.keySet().toArray()[2], (Number) final_result.get (final_result.keySet().toArray()[2]));
        chart.addSeries ((String) final_result.keySet().toArray()[3], (Number) final_result.get (final_result.keySet().toArray()[3]));
        chart.addSeries ((String) final_result.keySet().toArray()[4], (Number) final_result.get (final_result.keySet().toArray()[4]));

        new SwingWrapper(chart).displayChart ();


    }

    public void bar_chart(java.util.List<String> jobs_list, List<Long> counts_list) {


        CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).title ("job Histogram").xAxisTitle ("jobs").yAxisTitle ("counts").build ();
        // 2.Customize Chart
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        // 3.Series
        chart.addSeries ("job counts", jobs_list,counts_list);
        // 4.Show it
        new SwingWrapper (chart).displayChart ();
    }


}
