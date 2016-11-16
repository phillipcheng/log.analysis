package bdap.util.test;


import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.junit.Test;

public class TestShowIps {

	@Test
    public void testShowIps() {
        try {
        InetAddress localhost = InetAddress.getLocalHost();
        System.out.println(" IP Addr: " + localhost.getHostAddress());
        // Just in case this host has multiple IP addresses....
        InetAddress[] allMyIps = InetAddress.getAllByName(localhost.getCanonicalHostName());
        if (allMyIps != null && allMyIps.length > 1) {
          System.out.println(" Full list of IP addresses:");
          for (int i = 0; i < allMyIps.length; i++) {
            System.out.println("    " + allMyIps[i].getHostAddress());
          }
        }
      } catch (UnknownHostException e) {
        System.out.println(" (error retrieving server host name)");
      }

      try {
        System.out.println("Full list of Network Interfaces:");
        for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
          NetworkInterface intf = en.nextElement();
          System.out.println("    " + intf.getName() + " " + intf.getDisplayName());
          for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
            System.out.println("        " + enumIpAddr.nextElement().toString());
          }
        }
      } catch (SocketException e) {
        System.out.println(" (error retrieving network interface list)");
      }
    }

}
