/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.javaopencvbook.csce561;

/**
 *
 * @author Akash
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache
        .hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author akash
 */    
    
    public class InReducer extends Reducer<Text, Text, Text, Text> 
    {
           int  fal = 0;
        private final static IntWritable id = new IntWritable(1);
        private OutputCollector<Text,IntWritable> oc ;
        Map<String,Map<String,String>> tm1 = new TreeMap<>();
            Map<String,Map<String,Double>> tf1 = new TreeMap<>();
            Map<String,Map<String,Double>> tidf5 = new TreeMap<>();
            
            Map<String,Map<String,Double>> tidf1 = new TreeMap<>();
            Map<String,Double> mag_doc = new TreeMap<>();
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
        {
            fal++;
            String[] query = {"akash","america","astonishment","blackmail","apple"};
            Text k1 = key;
            int sum = 0;
            Map<String,Integer> hm = new TreeMap();
            Map<String,Double> tmag_doc = new TreeMap<>();
            int counter = 1;
            for (Text val : values) 
            {
                String st = val.toString();
                String[] sp=st.split("\\@");
                int it = (Integer.parseInt(sp[0]));
                
                sum  =sum+ it;                
                if(!hm.containsKey(sp[1]))
                {
                    hm.put(sp[1], counter);
                    
                }
                else
                {               
                    counter=(int)hm.get(sp[1]);
                    counter++;
                    hm.remove(sp[1]);
                    hm.put(sp[1], counter);
                }              
                context.write(key,new Text("|"+sp[1]+"|"));
            }
            
            double Fsum =0.0;
            for(String fg : hm.keySet())
            {
                double kok = (double)(hm.get(fg));
                Fsum+=Math.pow(kok, 2);
                if(mag_doc.containsKey(fg))
                {
                    double lp = mag_doc.get(fg);
                    lp+=Fsum;
                }
                else
                {
                    mag_doc.put(fg, Fsum);
                }
            }
            
            ArrayList<String> al = new ArrayList();
            String y = k1.toString();
            for(int u=0;u<query.length;u++)
            {
                Stemmer st1 = new Stemmer();
                String temp1 = query[u];
                char[] inchar = temp1.toCharArray();
                for(char temp: inchar)
                {
                    temp = Character.toLowerCase((char) temp);
                    st1.add(temp);                                 
                }                             
                st1.stem();
                al.add(st1.toString());                
            }
//            double size3 = 5000.0;
            double size3 = 3.0;
            for(String name : hm.keySet() )
            {
                String entered=new String();
                Map<String,String> ttm = new TreeMap<>();
                Map<String,Double> ttm1 = new TreeMap<>();
                String key1 = name;
                String value1 = hm.get(name).toString();
                int yi = Integer.parseInt(value1);
                double ui =1+ Math.log10(yi);
                ttm.put(key1, value1);
                ttm1.put(key1, ui);
                entered = key1+"/"+Double.toString(ui);
                context.write(new Text(entered),new Text("/") );
                if(tm1.containsKey(y)||tf1.containsKey(y))
                {
                    Map<String,String> tt1 = tm1.get(y);
                    Map<String,Double> tt2 = tf1.get(y);                                      
                    tt2.put(key1, ui);
                    tt1.put(key1, value1);                                        
                    tm1.put(y, tt1);
                    tf1.put(y, tt2);
                    
                }
                else
                {
                    tf1.put(y, ttm1);
                    tm1.put(y, ttm);
                }
                
                
                context.write(new Text("added y: "+y), new Text(""));
            }
//            for(String sx : tm1.keySet())
//                {
//                    context.write(new Text(sx), new Text("/n"));
//                    Map<String,String> iiii = tm1.get(sx);
//                    for(String jk : iiii.keySet())
//                    context.write(new Text(jk),new Text(iiii.get(jk)));
//                }
             tidf5 = tf1;
//            Map<String,Map<String,Double>> tidf1 = new TreeMap<>();
            
            if(fal==3)
            {
            for(int i=0;i<al.size();i++)
                {
                    for(String sd : tidf5.keySet())
                        {     
                             if(al.get(i).equals(sd))
                             {
                                 
                                Map<String,Double> ki = tidf5.get(sd);
                                double size1 = ki.size();
                                for(String sd1 : ki.keySet())
                                {
                                    
                                    double kp = ki.get(sd1);
                                    double ji =(double)((kp)*((double) Math.log10((size3)/(size1))));                                     
                                    ki.put(sd1, ji);  
                                    context.write(new Text(Double.toString(ji)),new Text(Double.toString(kp)));
                                }
                                tidf1.put(al.get(i), ki);
                            }                         
                             
                        }
                }
            
            for(String gh : tidf1.keySet())
            {
                Map<String,Double> km = tidf1.get(gh);
                for(String gh1 : km.keySet())
                {
                    String id = gh1;
                    String weight = Double.toString(km.get(id));
                }
            }
            int Fcount =1;
            String jkll = new String();
            String jkl = new String();
            Map<String,Map<String,String>> Final = new TreeMap<>();
            Map<String,String> icos1 = new TreeMap<>();
            String f_id1= new String();
//            if(fal.size()==1)
//            {
            for(int i=0;i<al.size();i++)
            {
                Fcount++;
                double tempCossim=0.0;
                String t_id=new String();
                for(String term : tidf1.keySet())
                {
                    Map<String,Double> id_count_w = new TreeMap<>();
                    Map<String,String> id_weight_d = new TreeMap<>();
                    Map<String,String> icos = new TreeMap<>();
                    
                    double cossim=0;
                    if(al.get(i).equals(term))
                    {
                        double dot_product =0.0;
                        double mag_product=0.0;
                        double mag_product1 =0.0;
                        double mag_product2 =0.0;
                        for(String fd : tm1.keySet())
                        {
                            
                        }
                        id_count_w = tidf1.get(term);
                        id_weight_d = tm1.get(term);
                        int count =1;
                        
                        for(String id : id_count_w.keySet())
                        {
                            
                            if(id_count_w.containsKey(id)==id_weight_d.containsKey(id))
                            {
                                context.write(new Text(id),new Text(Double.toString(id_count_w.get(id))));
                                count++;
                                int t_c = id_count_w.size();
                                double w_count = id_count_w.get(id);
                                double d_count = Double.parseDouble(id_weight_d.get(id));
                                dot_product+=(w_count*d_count);
                                mag_product1+=Math.pow(w_count,2);
//                                mag_product2+=Math.pow(d_count,2);                                                                
                                mag_product2+=mag_doc.get(id);
                            }
                            
                            f_id1= id;
                        }
                        context.write(new Text(Double.toString(dot_product)),new Text(Double.toString(mag_product1)));
                        mag_product=(double)((Math.sqrt(((mag_product1)*(mag_product2)))));
                        cossim =(double)(dot_product/mag_product);
                        context.write(new Text(Double.toString(cossim)),new Text(al.get(i)));
                        if(cossim>tempCossim)
                        {
                            
                            tempCossim = cossim;
                            t_id=f_id1;
                            
                        }
                        if(Fcount==al.size())
                        {
                            jkl = Double.toString(cossim);
                            jkll = "Maximum cosine similarity="+(f_id1);
                            context.write(new Text(jkll),new Text(jkl));
                        }
                        
                    } 
                    
                }                
            }
            }
        }
        
    }