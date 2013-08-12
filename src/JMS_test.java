import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.varia.NullAppender;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class JMS_test {
	public static String topic_name = "Nagios_Test";
	
    public static void main(String[] args) throws Exception {
    	// Tell log4j to send it's logs to /dev/null
    	org.apache.log4j.BasicConfigurator.configure(new NullAppender());
    	
    	String my_jms_url = null;
    	
    	// Check if we 
    	if (args.length != 1) {
            System.out.println("You must specify a connection string");
            System.exit(2);   		
    	} else {
    		my_jms_url = args[0] + "?timeout=1000";
    	}
        thread(new myMessageProducer(my_jms_url), false);
        thread(new myMessageConsumer(my_jms_url), false);
    }
 
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
 
    public static class myMessageProducer implements Runnable  {
    	private String jms_url;

    	myMessageProducer(final String jms_url) {
            this.jms_url = jms_url;
          }
        
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jms_url);
 
                connectionFactory.setSendTimeout(5);
                
                // Create a Connection
                Connection connection = connectionFactory.createConnection();

                connection.start();
 
                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic(topic_name);
 
                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 
                // Create a messages
                String text = "Nagios test message";
                //String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                TextMessage message = session.createTextMessage(text);
 
                // Tell the producer to send the message
                //System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
                producer.send(message);
 
                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
                System.exit(2);
            }
        }
    }
 
    public static class myMessageConsumer implements Runnable, ExceptionListener {
    	private String jms_url;

    	myMessageConsumer(final String jms_url) {
            this.jms_url = jms_url;
          }
    	
        public void run() {
            try {
 
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jms_url);
 
                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                connection.setExceptionListener(this);
 
                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic(topic_name);
 
                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);
 
                // Wait for a message
                Message message = consumer.receive(1000);
 
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Received: " + text);
                    System.exit(0);
                } else {
                    System.out.println("ERROR: Did not receive message");
                    System.exit(2);
                }
 
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
                System.exit(2);
            }
        }
 
        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
            System.exit(2);
        }
    }

}
