package uk.ac.imperial.lsds.saber.experiments.microbenchmarks;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatMultiplication;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.NoOp;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.NoOpKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

public class TestGDB {
	
	public static final String usage = "usage: TestGDB";
	
	public static void main (String [] args) throws Exception {
		
		/* batch config */
		int batchSize = 1048576;
		
		/* windows config */
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;

		// WindowType windowType1 = WindowType.ROW_BASED;
		// int windowRange1 = 1024;
		// int windowSlide1 = 1024;

		/* buffer config */
		int tuplesPerInsert = 16384; /* (16 x 1024) x 64 = 1MB */
		int ntasks = 1024; // 8812;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (! SystemConf.parse(args[i], args[j])) {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
		SystemConf.dump();
		
		/*
		SystemConf.CIRCULAR_BUFFER_SIZE = 512 * 1048576;
		SystemConf.LATENCY_ON = false;
		
		SystemConf.PARTIAL_WINDOWS = 0;
		
		SystemConf.THROUGHPUT_MONITOR_INTERVAL  = 200L;
		SystemConf.PERFORMANCE_MONITOR_INTERVAL = 500L;
		
		SystemConf.SCHEDULING_POLICY = SchedulingPolicy.HLS;
		
		SystemConf.SWITCH_THRESHOLD = 10;
		
		SystemConf.CPU = false;
		SystemConf.GPU = false;
		*/
		
		/*
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;
		*/
		
		/* Query init */
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);
		
		int [] offsets = new int [12];
		
		offsets[ 0] =  0; /*   timestamp:  long */
		offsets[ 1] =  8; /*       jobId:  long */
		offsets[ 2] = 16; /*      taskId:  long */
		offsets[ 3] = 24; /*   machineId:  long */
		offsets[ 4] = 32; /*      userId:   int */
		offsets[ 5] = 36; /*   eventType:   int */
		offsets[ 6] = 40; /*    category:   int */
		offsets[ 7] = 44; /*    priority:   int */
		offsets[ 8] = 48; /*         cpu: float */
		offsets[ 9] = 52; /*         ram: float */
		offsets[10] = 56; /*        disk: float */
		offsets[11] = 60; /* constraints:   int */
		
		ITupleSchema schema = new TupleSchema (offsets, 64);
		
		schema.setAttributeType ( 0, PrimitiveType.LONG );
		schema.setAttributeType ( 1, PrimitiveType.LONG );
		schema.setAttributeType ( 2, PrimitiveType.LONG );
		schema.setAttributeType ( 3, PrimitiveType.LONG );
		schema.setAttributeType ( 4, PrimitiveType.INT  );
		schema.setAttributeType ( 5, PrimitiveType.INT  );
		schema.setAttributeType ( 6, PrimitiveType.INT  );
		schema.setAttributeType ( 7, PrimitiveType.INT  );
		schema.setAttributeType ( 8, PrimitiveType.FLOAT);
		schema.setAttributeType ( 9, PrimitiveType.FLOAT);
		schema.setAttributeType (10, PrimitiveType.FLOAT);
		schema.setAttributeType (11, PrimitiveType.INT  );


		// QueryConf queryConf1 = new QueryConf (batchSize);
		
		// WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
		
		// int [] offsets1 = new int [12];
		
		// offsets1[ 0] =  0; /*   timestamp:  long */
		// offsets1[ 1] =  8; /*       jobId:  long */
		// offsets1[ 2] = 16; /*      taskId:  long */
		// offsets1[ 3] = 24; /*   machineId:  long */
		// offsets1[ 4] = 32; /*      userId:   int */
		// offsets1[ 5] = 36; /*   eventType:   int */
		// offsets1[ 6] = 40; /*    category:   int */
		// offsets1[ 7] = 44; /*    priority:   int */
		// offsets1[ 8] = 48; /*    sum(cpu): float */
		// offsets1[ 9] = 52; /*         ram: float */
		// offsets1[10] = 56; /*        disk: float */
		// offsets1[11] = 60; /* constraints:   int */
		
		// ITupleSchema schema1 = new TupleSchema (offsets1, 64);
		
		// schema1.setAttributeType ( 0, PrimitiveType.LONG );
		// schema1.setAttributeType ( 1, PrimitiveType.LONG );
		// schema1.setAttributeType ( 2, PrimitiveType.LONG );
		// schema1.setAttributeType ( 3, PrimitiveType.LONG );
		// schema1.setAttributeType ( 4, PrimitiveType.INT  );
		// schema1.setAttributeType ( 5, PrimitiveType.INT  );
		// schema1.setAttributeType ( 6, PrimitiveType.INT  );
		// schema1.setAttributeType ( 7, PrimitiveType.INT  );
		// schema1.setAttributeType ( 8, PrimitiveType.FLOAT);
		// schema1.setAttributeType ( 9, PrimitiveType.FLOAT);
		// schema1.setAttributeType (10, PrimitiveType.FLOAT);
		// schema1.setAttributeType (11, PrimitiveType.INT  );

		/* Load attributes of interest */
		int extraBytes = 5120 * schema.getTupleSize();
		
		ByteBuffer [] data = new ByteBuffer [ntasks];
		for (i = 0; i < ntasks; i++)
			data[i] = ByteBuffer.allocate(schema.getTupleSize() * tuplesPerInsert);
		
		ByteBuffer buffer;
		
		for (i = 0; i < ntasks; i++) {
			buffer = data[i];
			buffer.clear();
			while (buffer.hasRemaining()) {
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putInt   (1);
				buffer.putInt   (1); // Event type
				buffer.putInt   (1); // Category
				buffer.putInt   (1); // Priority
				buffer.putFloat (1); // CPU
				buffer.putFloat (1);
				buffer.putFloat (1);
				buffer.putInt   (1);
			}
		}
		
		String dataDir = SystemConf.SABER_HOME + "/datasets/google-cluster-data/";
		
		String [] filenames = {
			dataDir +"norm-event-types.txt",
			dataDir +      "categories.txt",
			dataDir +      "priorities.txt",
			dataDir + "cpu-utilisation.txt",
		};
		
		boolean [] containsInts = { true, true, true, false };
		
		FileInputStream f;
		DataInputStream d;
		BufferedReader  b;
		
		String line = null;
		int lines = 0;
		
		int bufferIndex, tupleIndex, attributeIndex;
			
		for (i = 0; i < 4; i++) {

			lines = 0;

			bufferIndex = tupleIndex = 0;
			
			buffer = data[bufferIndex];

			/* Load file into memory */

			System.out.println(String.format("# loading file %s", filenames[i]));
			f = new FileInputStream(filenames[i]);
			d = new DataInputStream(f);
			b = new BufferedReader(new InputStreamReader(d));

			while ((line = b.readLine()) != null) {

				if (tupleIndex >= tuplesPerInsert) {
					tupleIndex = 0;
					bufferIndex ++;

					/* Zijian: Would cause out of Index range if ntask if ntasks too small */
					if (bufferIndex >= ntasks) {
						break;
					}
				}

				/* use line number as timestamp */
				if (i == 0) { /* only insert timestamp for once */
					buffer.putLong(tupleIndex *schema.getTupleSize() + 0, Integer.toUnsignedLong(lines));
				} 
				lines += 1;

				buffer = data[bufferIndex];

				attributeIndex = tupleIndex * schema.getTupleSize() + 36 + (i * 4);
				
				if (containsInts[i])
					buffer.putInt(attributeIndex, Integer.parseInt(line));
				else
					buffer.putFloat(attributeIndex, Float.parseFloat(line));
				
				tupleIndex ++;
			}

			b.close();

			System.out.println(String.format("# %d lines last buffer position at %d has remaining ? %5s (%d bytes)", 
					lines, buffer.position(), buffer.hasRemaining(), buffer.remaining()));

			/* Fill in the extra lines */
			if (i != 0) {
				int destPos = buffer.capacity() - extraBytes;
				System.arraycopy(buffer.array(), 0, buffer.array(), destPos, extraBytes);
			}
		}

		/* timestamp reference for latency */
		long timestampReference = System.nanoTime();
		
		/* query setup */
		AggregationType [] aggregationTypes = new AggregationType [1];
		for (i = 0; i < aggregationTypes.length; ++i) {
			aggregationTypes[i] = AggregationType.fromString("sum"); // sum()
		}
		
		FloatColumnReference [] aggregationAttributes = new FloatColumnReference [1];
		for (i = 0; i < aggregationAttributes.length; ++i) {
			aggregationAttributes[i] = new FloatColumnReference(8); // sum(cpu)
		}

		Expression [] groupByAttributes = new Expression [] {new IntColumnReference(6)}; // group by category

		IOperatorCode cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes);
		IOperatorCode gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, groupByAttributes, schema, batchSize);

		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);

		Query query = new Query (0, operators, schema, window, null, null, queryConf, timestampReference);


		// Expression [] expressions = new Expression [3];
		// /* Always project the timestamp */
		// expressions[0] = new  LongColumnReference(0);
		// expressions[1] = new IntColumnReference(6);
		// expressions[2] = new FloatColumnReference(8);
		
		// IOperatorCode cpuCode1 = new Projection (expressions);
		// IOperatorCode gpuCode1 = new ProjectionKernel (schema1, expressions, batchSize, 1);
		// QueryOperator operator1;
		// operator1 = new QueryOperator (cpuCode1, gpuCode1);
		// Set<QueryOperator> operators1 = new HashSet<QueryOperator>();
		// operators1.add(operator1);
		// Query query1 = new Query (1, operators1, schema1, window1, null, null, queryConf1, timestampReference);
		

		// query.connectTo(query1);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query);
		// queries.add(query1);
		
		/* application setup */
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		/* Can only been put her */ 
		/* The path is query -> dispatcher -> handler -> aggregator */
		if (SystemConf.CPU)
			query.setAggregateOperator((IAggregateOperator) cpuCode);
		else
			query.setAggregateOperator((IAggregateOperator) gpuCode);

		/* Push the input stream */		
		i = 0;
		while (true) {
			application.processData (data[i].array());
			i ++;
			i = i % ntasks;
		}
	}
}
