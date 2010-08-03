--- ns-2.33/tcp/tcp.cc	2009-06-06 17:58:35.000000000 -0400
+++ orig.ns-2.33/tcp/tcp.cc	2008-03-31 22:00:28.000000000 -0400
@@ -614,12 +614,6 @@
         // RFC2988 allows a maximum for the backed-off RTO of 60 seconds.
         // This is applied by maxrto_.
 
-	if (t_backoff_ < 0)
-	{
-		t_backoff_ >>= 1;
-		t_backoff_ = -t_backoff_;
-	}
-
 	if (t_backoff_ > 8) {
 		/*
 		 * If backed off this far, clobber the srtt
