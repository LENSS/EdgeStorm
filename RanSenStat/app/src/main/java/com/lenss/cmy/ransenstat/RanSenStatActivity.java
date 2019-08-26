package com.lenss.cmy.ransenstat;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.net.LocalServerSocket;
import android.net.LocalSocket;
import android.net.LocalSocketAddress;
import android.net.wifi.WifiManager;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Message;
import android.os.SystemClock;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.text.format.Formatter;
import android.view.View;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.masternode.Reply;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.StormSubmitter;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.Serialization;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import tools.Utils;

public class RanSenStatActivity extends AppCompatActivity {
    public final int KEYWORDS_NUM_SOCKET_PORT = 7654;
    public static String localAddress;

    public static final String MStormDir = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/";
    public static final String apkFileDirectory = MStormDir + "APK/";
    public static final String apkFileName = "RanSenStat.apk";
    private static final String E2E_RESPONSE_TIME_ADDRESS = apkFileDirectory + "E2EResponseTime";

    private static final String LOCAL_ADDRESS = "com.android.cmy.ransenstat";
    private static final String LOCAL_RESULT_ADDRESS = "com.android.cmy.ransenstat.result";


    public static String TOPIC = "apple";
    public static String KEYWORDS = "day";

    // stream input method and speed
    public static final int CONSINPUT = 1;
    public static final int UNIRANDINPUT = 2;
    public static final int GAUSSIANINPUT = 3;
    public static final int EXPINPUT = 4;

    public static int inputMethod = CONSINPUT;
    public static double avgIAT = 0.1;
    public static double minIAT = 0.1;
    public static double maxIAT = 0.1;

    // workload of each component
    public static int senDistributorWorkload = 1;
    public static int topicTaggerWorkload = 10;
    public static int keywordStatWorkload = 5;
    public static int senSinkWorkload = 1;
    //public static double variance_percent = 0.0;

    // initial parallelism of each component
    public static int senDistributorParallel = 1;
    public static int topicTaggerParallel = 2;
    public static int keywordStatParallel = 1;
    public static int senSinkParallel = 1;

    // initial stream grouping method of each component
    public static int topicTaggerGroupingMethod = Topology.Shuffle;
    public static int keywordStatGroupingMethod = Topology.Shuffle;
    public static int senSinkGroupingMethod = Topology.Shuffle;

    // schedule requirements of each component
    public static int senDistributorScheduleReq = Topology.Schedule_Local;
    public static int topicTaggerScheduleReq = Topology.Schedule_Any;
    public static int keywordStatScheduleReq = Topology.Schedule_Any;
    public static int senSinkScheduleReq = Topology.Schedule_Local;

    public static List<String> keywordList = null;
    public static final int NEW_SENTENCE = 0;
    public static final int STAT_RESULT = 1;

    private StormSubmitter submitter;
    private boolean mGeneratingEnabled = false;

    private TextView sentenceText;
    private TextView result;

    private int totalPackets = 0;

    private ConcurrentLinkedQueue<InternodePacket> sentenceQueue = new ConcurrentLinkedQueue<>();


    private sentenceGenerator senGenerator;

    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            if (msg.what == NEW_SENTENCE) {
                sentenceText = (TextView) findViewById(R.id.randomSentence_text);
                sentenceText.setText(msg.obj.toString());
            } else {
                String statResult = msg.obj.toString();
                result = (TextView) findViewById(R.id.keyWordStatResult_text);
                result.setText(statResult);
            }
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_ran_sen_stat);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        /// For Real Machines
        WifiManager wm = (WifiManager) getApplicationContext().getSystemService(WIFI_SERVICE);
        localAddress = Formatter.formatIpAddress(wm.getConnectionInfo().getIpAddress());

        /// For virtual machines
        //localAddress = Utils.getIPAddress(true);


        /// For container
        //localAddress = Utils.getIPAddress();

        new Thread(new ResultLocalServer()).start();

        new Thread(new StreamLocalServer()).start();


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_ran_sen_stat, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_set_genSpeed){
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);

            final EditText inputMethodBox = new EditText(this);
            inputMethodBox.setHint("IM(cons:1, uniRand:2, gaussian:3, expo:4)");
            layout.addView(inputMethodBox);

            final EditText avgspeedBox = new EditText(this);
            avgspeedBox.setHint("AverageIAT:"+ avgIAT + "s");
            layout.addView(avgspeedBox);

            final EditText minspeedBox = new EditText(this);
            minspeedBox.setHint("MinIAT:"+ minIAT + "s");
            layout.addView(minspeedBox);

            final EditText maxspeedBox = new EditText(this);
            maxspeedBox.setHint("MaxIAT:"+ maxIAT+ "s");
            layout.addView(maxspeedBox);

            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    avgIAT= Double.parseDouble(avgspeedBox.getText().toString());
                    minIAT = Double.parseDouble(minspeedBox.getText().toString());
                    maxIAT = Double.parseDouble(maxspeedBox.getText().toString());
                    inputMethod = Integer.parseInt(inputMethodBox.getText().toString());
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else if (id == R.id.action_set_workloads) {
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);

            final EditText senDistributorBox = new EditText(this);
            senDistributorBox.setHint("SenDistributor:"+senDistributorWorkload);
            layout.addView(senDistributorBox);

            final EditText topicTaggerBox = new EditText(this);
            topicTaggerBox.setHint("TopicTagger:"+topicTaggerWorkload);
            layout.addView(topicTaggerBox);

            final EditText keywordStatBox = new EditText(this);
            keywordStatBox.setHint("KeywordStat:"+keywordStatWorkload);
            layout.addView(keywordStatBox);

            final EditText senSinkBox = new EditText(this);
            senSinkBox.setHint("SenSink:"+senSinkWorkload);
            layout.addView(senSinkBox);

            /*final EditText variancePercentBox = new EditText(this);
            variancePercentBox.setHint("variancePercent:"+variance_percent);
            layout.addView(variancePercentBox);*/
            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    senDistributorWorkload = Integer.parseInt(senDistributorBox.getText().toString());
                    topicTaggerWorkload = Integer.parseInt(topicTaggerBox.getText().toString());
                    keywordStatWorkload = Integer.parseInt(keywordStatBox.getText().toString());
                    senSinkWorkload = Integer.parseInt(senSinkBox.getText().toString());
                    //variance_percent = Double.parseDouble(variancePercentBox.getText().toString());
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else if (id == R.id.action_set_parallelism) {
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);

            final EditText senDistributorBox = new EditText(this);
            senDistributorBox.setHint("SenDistributor:"+senDistributorParallel);
            layout.addView(senDistributorBox);

            final EditText topicTaggerBox = new EditText(this);
            topicTaggerBox.setHint("TopicTagger:"+topicTaggerParallel);
            layout.addView(topicTaggerBox);

            final EditText keywordStatBox = new EditText(this);
            keywordStatBox.setHint("KeywordStat:"+keywordStatParallel);
            layout.addView(keywordStatBox);

            final EditText senSinkBox = new EditText(this);
            senSinkBox.setHint("SenSink:"+senSinkParallel);
            layout.addView(senSinkBox);

            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    senDistributorParallel = Integer.parseInt(senDistributorBox.getText().toString());
                    topicTaggerParallel = Integer.parseInt(topicTaggerBox.getText().toString());
                    keywordStatParallel = Integer.parseInt(keywordStatBox.getText().toString());
                    senSinkParallel = Integer.parseInt(senSinkBox.getText().toString());
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else if (id == R.id.action_set_groupingMethod) {
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);

            final EditText topicTaggerBox = new EditText(this);
            topicTaggerBox.setHint("TopicTagger (shuffle:1, feedbackBased:2):" + topicTaggerGroupingMethod);
            layout.addView(topicTaggerBox);

            final EditText keywordStatBox = new EditText(this);
            keywordStatBox.setHint("KeywordStat (shuffle:1, feedbackBased:2):" + keywordStatGroupingMethod);
            layout.addView(keywordStatBox);

            final EditText senSinkBox = new EditText(this);
            senSinkBox.setHint("SenSink (shuffle:1, feedbackBased:2):" + senSinkGroupingMethod);
            layout.addView(senSinkBox);

            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    topicTaggerGroupingMethod = Integer.parseInt(topicTaggerBox.getText().toString());
                    keywordStatGroupingMethod = Integer.parseInt(keywordStatBox.getText().toString());
                    senSinkGroupingMethod = Integer.parseInt(senSinkBox.getText().toString());

                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else {
                keywordList = Arrays.asList(KEYWORDS.split("\\s*,\\s*"));
                Topology topology = RanSenStatTopology.createTopology();
                submitter = new StormSubmitter(this, apkFileDirectory);
                submitter.submitTopology(apkFileName, topology);
                // wait for reply containing topologyID
                Reply reply;
                while((reply=submitter.getReply())==null){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                int topologyID = new Integer(reply.getContent());
                if(topologyID != 0)
                    Toast.makeText(this, "Topology Scheduled!", Toast.LENGTH_SHORT).show();
        }
        return super.onOptionsItemSelected(item);
    }

    public void clickButtonGenerateSentence(View unused) {
        mGeneratingEnabled = !mGeneratingEnabled;
        Button genSenButton = (Button) findViewById(R.id.generateSentence_button);
        int id = mGeneratingEnabled ?
                R.string.button_stopGenerate : R.string.button_generateSentence;
        genSenButton.setText(id);

        if (mGeneratingEnabled) {
            if (senGenerator == null) {
                senGenerator = new sentenceGenerator(inputMethod, avgIAT, minIAT, maxIAT, mHandler);
                senGenerator.start();
            } else {
                senGenerator.resume();
            }
        } else {
            senGenerator.suspend();
        }
    }

    public static String getLocalAddress() {
        return localAddress;
    }

    class sentenceGenerator implements Runnable {           // Thread generating source data
        private Handler mHandler;
        private String[] sentences = new String[]{
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
                };
        private String sentence;
        private byte[] sentenceByte = null;
        private Random rand;
        private Thread t;

        private double inputMehod;
        private double avgInterArrivalTime;
        private double minInterArrivalTime;
        private double maxInterArrivalTime;

        boolean suspended = false;

        public sentenceGenerator(int inputMethod, double avgIAT, double minIAT, double maxIAT, Handler mHandler) {
            this.inputMehod = inputMethod;
            this.avgInterArrivalTime = avgIAT;
            this.minInterArrivalTime = minIAT;
            this.maxInterArrivalTime = maxIAT;
            this.mHandler = mHandler;
            rand = new Random();
        }

        public void run() {
            if(inputMehod == CONSINPUT) {
                constantInput(avgInterArrivalTime);
            } else if (inputMehod == UNIRANDINPUT) {
                unifiedRandomInput(minInterArrivalTime, maxInterArrivalTime);
            } else if (inputMehod == GAUSSIANINPUT) {
                gaussianInput(minInterArrivalTime,maxInterArrivalTime);
            } else {
                exponentialInput(minInterArrivalTime, maxInterArrivalTime, avgInterArrivalTime);
            }
        }

        public void constantInput(double avgIAT){
            long avgInterTime = Double.valueOf(avgIAT*1000000000).longValue();
            long millSleepTime = avgInterTime/1000000;
            int nanoSleepTime = (int) avgInterTime%1000000;

            while (!Thread.currentThread().isInterrupted()) {
                sentence = sentences[rand.nextInt(sentences.length)];
                mHandler.obtainMessage(NEW_SENTENCE, sentence).sendToTarget();
                InternodePacket pkt = new InternodePacket();
                pkt.ID = System.nanoTime();
                pkt.simpleContent.put("sentence", sentence);
                sentenceQueue.add(pkt);
                Utils.highPrecisionSleep(millSleepTime,nanoSleepTime);
                synchronized (this) {
                    while (suspended) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public void unifiedRandomInput(double minIAT, double maxIAT){
            double distance = maxIAT - minIAT;

            while (!Thread.currentThread().isInterrupted()) {
                sentence = sentences[rand.nextInt(sentences.length)];
                mHandler.obtainMessage(NEW_SENTENCE, sentence).sendToTarget();
                InternodePacket pkt = new InternodePacket();
                pkt.ID = System.nanoTime();
                pkt.simpleContent.put("sentence", sentence);
                sentenceQueue.add(pkt);
                long randIAT = Double.valueOf((rand.nextDouble() * distance + minIAT)*1000000000).longValue();
                long millSleepTime = randIAT/1000000;
                int nanoSleepTime = (int) randIAT%1000000;
                Utils.highPrecisionSleep(millSleepTime,nanoSleepTime);
                synchronized (this) {
                    while (suspended) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public void gaussianInput(double minIAT, double maxIAT){
            while (!Thread.currentThread().isInterrupted()) {
                sentence = sentences[rand.nextInt(sentences.length)];
                mHandler.obtainMessage(NEW_SENTENCE, sentence).sendToTarget();
                InternodePacket pkt = new InternodePacket();
                pkt.ID = System.nanoTime();
                pkt.simpleContent.put("sentence", sentence);
                sentenceQueue.add(pkt);
                double randIAT = rand.nextGaussian()*(maxIAT-minIAT)/6 + (minIAT + maxIAT)/2;
                while (!((randIAT >= minIAT) && (randIAT <= maxIAT))) {
                    randIAT = rand.nextGaussian()*(maxIAT-minIAT)/6 + (minIAT + maxIAT)/2;
                }

                long randInterTime = Double.valueOf(randIAT*1000000000.0).longValue();
                long millSleepTime = randInterTime/1000000;
                int nanoSleepTime = (int) randInterTime%1000000;
                Utils.highPrecisionSleep(millSleepTime,nanoSleepTime);

                synchronized (this) {
                    while (suspended) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public void exponentialInput(double minIAT, double maxIAT, double avgIAT){
            while (!Thread.currentThread().isInterrupted()) {
                sentence = sentences[rand.nextInt(sentences.length)];
                mHandler.obtainMessage(NEW_SENTENCE, sentence).sendToTarget();
                InternodePacket pkt = new InternodePacket();
                pkt.ID = System.nanoTime();
                pkt.simpleContent.put("sentence", sentence);
                sentenceQueue.add(pkt);
                double randIAT = - Math.log(rand.nextDouble()) * avgIAT;
                while (!((randIAT >= minIAT) && (randIAT <= maxIAT))) {
                    randIAT = - Math.log(rand.nextDouble()) * avgIAT;
                }
                long randInterTime = Double.valueOf(randIAT*1000000000.0).longValue();
                long millSleepTime = randInterTime/1000000;
                int nanoSleepTime = (int) randInterTime%1000000;
                Utils.highPrecisionSleep(millSleepTime,nanoSleepTime);

                synchronized (this) {
                    while (suspended) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

        }

        public void start() {
            if (t == null) {
                t = new Thread(this);
                t.start();
            }
        }

        public void suspend() {
            suspended = true;
        }

        synchronized public void resume() {
            suspended = false;
            notify();
        }
    }

    class ResultLocalServer implements Runnable {          // Thread waiting for connection request to report results
        private LocalServerSocket localServerSocket;
        private LocalSocket localClientSocket;

        public void run() {
            System.out.println("[Result Local Server] has been set up ...");
            try {
                localServerSocket = new LocalServerSocket(LOCAL_RESULT_ADDRESS);
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    localClientSocket = localServerSocket.accept();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                new Thread(new ResultUpdateThreadLocal(localClientSocket, mHandler)).start();
                System.out.println("A new [local result update thread] starts ... ");
            }
        }
    }

    class ResultUpdateThreadLocal implements Runnable {    // Thread for collecting report results and show it on screen
        LocalSocket client;
        private DataInputStream input;
        private Handler mHandler;
        private boolean connected;
        FileWriter fw;

        public ResultUpdateThreadLocal(LocalSocket client, Handler mHandler) {
            this.client = client;
            this.mHandler = mHandler;
            connected = true;
        }

        @Override
        public void run() {
            try {
                this.input = new DataInputStream(client.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (connected) {

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int count = 0;
                try {
                    count = input.available();
                } catch (IOException e){
                    connected = false;
                    e.printStackTrace();
                }
                if (count > 0) {
                    byte[] pktByte = new byte[count];
                    try {
                        input.read(pktByte);
                    } catch (IOException e) {
                        connected = false;
                        e.printStackTrace();
                    }

                    String strPkt = null;
                    try {
                        strPkt = new String(pktByte, "UTF-8");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    InternodePacket pkt = (InternodePacket) Serialization.Deserialize(strPkt, InternodePacket.class);

                    totalPackets++;
                    String statResult = "#processedPkt:" + totalPackets;
                    mHandler.obtainMessage(STAT_RESULT, statResult).sendToTarget();

                    try {
                        fw = new FileWriter(E2E_RESPONSE_TIME_ADDRESS + Thread.currentThread().getId(), true);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    Long startTime = pkt.ID;
                    double responseTime = (SystemClock.elapsedRealtimeNanos() - startTime) / 1000000.0;  // ms
                    String report = "StartTime:\t" + String.format("%.0f", startTime / 1000000000.0) + "\t"
                                + "E2EResponseTime:\t" + String.format("%.1f", responseTime) + "\n";

                    try {
                        fw.write(report);
                        fw.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class StreamLocalServer implements Runnable{
        private LocalServerSocket localServerSocket;
        private LocalSocket localClientSocket;

        public void run(){
            try {
                localServerSocket = new LocalServerSocket(LOCAL_ADDRESS);
                System.out.println("[Stream Local Server] has been set up ...");
            } catch (IOException e) {
                e.printStackTrace();
            }

            while(!Thread.currentThread().isInterrupted()){
                try{
                    localClientSocket = localServerSocket.accept();
                } catch (IOException e){
                    e.printStackTrace();
                }

                new Thread(new StreamThreadLocal(localClientSocket)).start();
                System.out.println("A new [local stream thread] starts ... ");
            }
        }
    }

    class StreamThreadLocal implements Runnable {
        private DataOutputStream os;
        private LocalSocket localClientSocket;
        private boolean connected;

        public StreamThreadLocal(LocalSocket clientSkt){
            localClientSocket = clientSkt;
            connected = true;
        }

        public void run() {
            try {
                os = new DataOutputStream(localClientSocket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (!Thread.currentThread().isInterrupted() && connected) {
                InternodePacket pkt = sentenceQueue.poll();
                if (pkt != null) {
                    try {
                        String strPkt = Serialization.Serialize(pkt);
                        byte[] pktByte = strPkt.getBytes("UTF-8");
                        os.write(pktByte);
                    } catch (IOException e) {
                        connected = false;
                        System.out.println("The thread sending tuple to [local client socket] stops ... ");
                    }
                }
            }
        }
    }
}
