package distributed.utils;

import java.io.*;
import java.net.*;

// 网络通信工具类
public class NetworkUtils {

    // Jobtracker地址和默认端口号
    public static int JOB_TRACKER_PORT = 9001;
    public static final String LOCALHOST = "127.0.0.1";
    public static void setJobTrackerPort(int port) {
        JOB_TRACKER_PORT = port;
    }
    
    // 发送对象到指定地址和端口
    public static void sendObject(Object obj, String host, int port) throws IOException {
        Socket socket = null;
        ObjectOutputStream out = null;
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(obj);
            out.flush();
        } finally {
            if (out != null) out.close();
            if (socket != null) socket.close();
        }
    }
    
    // 从ServerSocket接收对象
    public static Object receiveObject(ServerSocket serverSocket) throws IOException, ClassNotFoundException {
        Socket socket = null;
        ObjectInputStream in = null;
        try {
            socket = serverSocket.accept();
            in = new ObjectInputStream(socket.getInputStream());
            return in.readObject();
        } finally {
            if (in != null) in.close();
            if (socket != null && !socket.isClosed()) socket.close();
        }
    }
}
