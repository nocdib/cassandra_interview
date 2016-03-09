package interview;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OutputResults {
	Session session;
	String [] outputMetrics;
	String outputFile;

	public OutputResults(Session session, String [] outputMetrics, String outputFile) {

		this.session = session;
		this.outputMetrics = outputMetrics;
		this.outputFile = outputFile;

		StringBuilder sb = new StringBuilder();
	    sb.append("<html>\n");
	    sb.append("<head>\n");
	    sb.append("<script type=\"text/javascript\"\n");
	    sb.append("src=\"https://www.google.com/jsapi?autoload={\n");
	    sb.append("'modules':[{\n");
	    sb.append("'name':'visualization',\n");
	    sb.append("'version':'1',\n");
	    sb.append("'packages':['corechart']\n");
	    sb.append("}]\n");
	    sb.append("}\"></script>\n");
	    sb.append("<script type=\"text/javascript\">\n");
	    sb.append("google.setOnLoadCallback(drawChart);\n");
	    sb.append("function drawChart() {\n");

	    for(String metric: outputMetrics){
	    	//count the number of records in the table and create an array of that length
	    	ResultSet res = session.execute("select count (*) from " + metric);
	 		Row row = res.one();
	 		int count = (int) row.getLong(0);
	 		long [] longData = new long[count+1]; // array for long metric values
	 		double [] doubleData = new double[count+1]; // array for double metric values

		    sb.append("var " + metric +"= google.visualization.arrayToDataTable([\n");

		    switch(metric){
		    	case "LiveSSTableCount":
		    		sb.append("['Time', 'Tables'],\n");
		    		break;
		    	case "x95thPercentile":
		    		sb.append("['Time', 'Microseconds'],\n");
		    		break;
		    	default:
		    		sb.append("['Time', 'Bytes'],\n");
		    		break;
		    }


		    //Store the metrics from the table into an array
	    	res = session.execute("select * from " + metric);
	 		row = res.one();

	 		// x95thpercentile is a double value while the others are long values
	 		switch(metric){
	 			case "x95thPercentile":
	 				while (row != null) {
	 		 			doubleData[row.getInt(0)] = (double) row.getDouble(1);
	 		 			row = res.one();
	 		 	    }
	 		 		for(int i=1; i<count+1; i++)
	 		 			sb.append("['" + i + "', " + doubleData[i] + "],\n");
	 		 		sb.append("]);\n");
	 		 		break;
	 		 	default:
	 		 		while (row != null) {
	 		 			longData[row.getInt(0)] = (long) row.getLong(1);
	 		 			row = res.one();
	 		 	    }
	 		 		for(int i=1; i<count+1; i++)
	 		 			sb.append("['" + i + "', " + longData[i] + "],\n");
	 		 		sb.append("]);\n");
	 		 		break;
	 		}
	    }

	    sb.append("var AllMemtablesHeapSizeOptions = {\n");
	    sb.append("title: 'AllMemtablesHeapSize',\n");
	    sb.append("hAxis: {title: 'Time (seconds)'},\n");
	    //sb.append("vAxis: {title: 'vAxis'},\n");
	    sb.append("curveType: 'function',\n");
	    sb.append("legend: { position: 'bottom' }\n");
	    sb.append("};\n");

	    sb.append("var AllMemtablesLiveDataSizeOptions = {\n");
	    sb.append("title: 'AllMemtablesLiveDataSize',\n");
	    sb.append("hAxis: {title: 'Time (seconds)'},\n");
	    //sb.append("vAxis: {title: 'vAxis'},\n");
	    sb.append("curveType: 'function',\n");
	    sb.append("legend: { position: 'bottom' }\n");
	    sb.append("};\n");

	    sb.append("var LiveSSTableCountOptions = {\n");
	    sb.append("title: 'LiveSSTableCount',\n");
	    sb.append("hAxis: {title: 'Time (seconds)'},\n");
	    //sb.append("vAxis: {title: 'vAxis'},\n");
	    sb.append("curveType: 'function',\n");
	    sb.append("legend: { position: 'bottom' }\n");
	    sb.append("};\n");

	    sb.append("var x95thPercentileOptions = {\n");
	    sb.append("title: '95thPercentile',\n");
	    sb.append("hAxis: {title: 'Time (seconds)'},\n");
	    //sb.append("vAxis: {title: 'vAxis'},\n");
	    sb.append("curveType: 'function',\n");
	    sb.append("legend: { position: 'bottom' }\n");
	    sb.append("};\n");

	    sb.append("var AllMemtablesHeapSizeChart = new google.visualization.LineChart(document.getElementById('AllMemtablesHeapSize'));\n");
	    sb.append("var AllMemtablesLiveDataSizeChart = new google.visualization.LineChart(document.getElementById('AllMemtablesLiveDataSize'));\n");
	    sb.append("var LiveSSTableCountChart = new google.visualization.LineChart(document.getElementById('LiveSSTableCount'));\n");
	    sb.append("var x95thPercentileChart = new google.visualization.LineChart(document.getElementById('x95thPercentile'));\n");

	    sb.append("AllMemtablesHeapSizeChart.draw(AllMemtablesHeapSize,AllMemtablesHeapSizeOptions);\n");
	    sb.append("AllMemtablesLiveDataSizeChart.draw(AllMemtablesLiveDataSize,AllMemtablesLiveDataSizeOptions);\n");
	    sb.append("LiveSSTableCountChart.draw(LiveSSTableCount,LiveSSTableCountOptions);\n");
	    sb.append("x95thPercentileChart.draw(x95thPercentile,x95thPercentileOptions);\n");

	    sb.append(" }\n");
	    sb.append("</script>\n");
	    sb.append("</head>\n");
	    sb.append("<body>\n");
	    sb.append("<div id=\"AllMemtablesHeapSize\" style=\"width: 900px; height: 500px\"></div>\n");
	    sb.append("<div id=\"AllMemtablesLiveDataSize\" style=\"width: 900px; height: 500px\"></div>\n");
	    sb.append("<div id=\"LiveSSTableCount\" style=\"width: 900px; height: 500px\"></div>\n");
	    sb.append("<div id=\"x95thPercentile\" style=\"width: 900px; height: 500px\"></div>\n");
	    sb.append("</body>\n");
	    sb.append("\n");
	    sb.append("\n");
	    sb.append("\n");
	    sb.append("\n");
	    sb.append("\n");
	    sb.append("\n");

	    FileWriter fstream;
		try {
			fstream = new FileWriter(outputFile);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(sb.toString());
		    out.close();
		} catch (IOException e) {
			System.out.println("Error outputing the metrics HTML output");
			System.exit(-1);
		}
	    System.out.println("Metrics output written to the file " + outputFile);
	}
}
