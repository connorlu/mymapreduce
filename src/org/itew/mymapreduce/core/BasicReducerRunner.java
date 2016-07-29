package org.itew.mymapreduce.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.itew.mymapreduce.io.Readable;
import org.itew.mymapreduce.io.Writable;

/**
 * 该类是一个reduce任务的抽象，它负责将结果收集
 * 
 * @author JiLu
 * 
 */
public class BasicReducerRunner<IK, IV, OK, OV> implements Runnable {

	private final Readable<IK, List<IV>> mappedDatas;
	private final Reducer<IK, IV, OK, OV> reducer;
	private final Writable<OK, OV> writer;

	public BasicReducerRunner(Readable<IK, List<IV>> mappedDatas,
			Reducer<IK, IV, OK, OV> reducer, Writable<OK, OV> writer) {
		this.mappedDatas = mappedDatas;
		this.reducer = reducer;
		this.writer = writer;
	}

	@Override
	public void run() {
		try {

			while (mappedDatas.next()) {

				reducer.reduce(mappedDatas.getKey(), mappedDatas.getValue(),writer);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			writer.close();
		}

	}

}
