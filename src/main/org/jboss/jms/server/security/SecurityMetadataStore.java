/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.server.security;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSSecurityException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.jboss.jms.server.SecurityStore;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.SubjectSecurityManager;

/**
 * A security metadate store for JMS. Stores security information for destinations and delegates
 * authentication and authorization to a JaasSecurityManager.
 *
 * @author Peter Antman
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SecurityMetadataStore implements SecurityStore
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityMetadataStore.class);

   public static final String SUCKER_USER = "JBM.SUCKER";
   
   public static final String DEFAULT_SUCKER_USER_PASSWORD = "CHANGE ME!!";
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private Map<String, SecurityMetadata> queueSecurityConf;
   private Map<String, SecurityMetadata> topicSecurityConf;

   private AuthenticationManager authenticationManager;
   private RealmMapping realmMapping;

   private String suckerPassword;

   private MessagingServer messagingServer;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public SecurityMetadataStore(MessagingServer messagingServer)
   {
      this.messagingServer = messagingServer;
      queueSecurityConf = new HashMap<String, SecurityMetadata>();
      topicSecurityConf = new HashMap<String, SecurityMetadata>();
      //add a property change listener then we can update the default security config
      messagingServer.getConfiguration().addPropertyChangeListener(new PropertyChangeListener()
         {
            public void propertyChange(PropertyChangeEvent evt)
            {
               if(evt.getPropertyName().equals("securityConfig"))
               {
                  HashSet<Role> roles = (HashSet<Role>) evt.getNewValue();
                  for (String key : queueSecurityConf.keySet())
                  {
                     if(evt.getNewValue() != null)
                        queueSecurityConf.put(key, new SecurityMetadata(roles));
                     else
                        queueSecurityConf.put(key, new SecurityMetadata());
                  }
                  for (String key : topicSecurityConf.keySet())
                  {
                     if(evt.getNewValue() != null)
                        topicSecurityConf.put(key, new SecurityMetadata(roles));
                     else
                        topicSecurityConf.put(key, new SecurityMetadata());
                  }
               }
            }
         });
   }

   // SecurityManager implementation --------------------------------

   public SecurityMetadata getSecurityMetadata(boolean isQueue, String destName)
   {
      SecurityMetadata m = (isQueue ? queueSecurityConf.get(destName) : topicSecurityConf.get(destName));

      if (m == null)
      {
         // No SecurityMetadata was configured for the destination, apply the default
         m = getDefaultSecurityConfig(destName);

      }
      return m;
   }

   private SecurityMetadata getDefaultSecurityConfig(String destName)
   {
      SecurityMetadata m;
      if (messagingServer.getConfiguration().getSecurityConfig() != null)
      {
         log.debug("No SecurityMetadadata was available for " + destName + ", using default security config");
         try
         {
            m = new SecurityMetadata(messagingServer.getConfiguration().getSecurityConfig());
         }
         catch (Exception e)
         {
            log.warn("Unable to apply default security for destName, using guest " + destName, e);
            m = new SecurityMetadata();
         }
      }
      else
      {
         // default to guest
         log.warn("No SecurityMetadadata was available for " + destName + ", adding guest");
         m = new SecurityMetadata();
      }
      return m;
   }

   public void setSecurityConfig(boolean isQueue, String destName, HashSet<Role> conf) throws Exception
   {
      if (trace) { log.trace("adding security configuration for " + (isQueue ? "queue " : "topic ") + destName); }
      
      if (conf == null)
      {
      	clearSecurityConfig(isQueue, destName);
      }
      else
      {	
	      SecurityMetadata m = new SecurityMetadata(conf);
	
	      if (isQueue)
	      {
	         queueSecurityConf.put(destName, m);
	      }
	      else
	      {
	         topicSecurityConf.put(destName, m);
	      }
      }
   }

   public void clearSecurityConfig(boolean isQueue, String name) throws Exception
   {
      if (trace) { log.trace("clearing security configuration for " + (isQueue ? "queue " : "topic ") + name); }

      if (isQueue)
      {
         queueSecurityConf.remove(name);
      }
      else
      {
         topicSecurityConf.remove(name);
      }
   }
   
   public Subject authenticate(String user, String password) throws JMSSecurityException
   {
      if (trace) { log.trace("authenticating user " + user); }
      
      SimplePrincipal principal = new SimplePrincipal(user);
      char[] passwordChars = null;
      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();
      
      boolean authenticated = false;
      
      if (SUCKER_USER.equals(user))
      {
      	if (trace) { log.trace("Authenticating sucker user"); }
      	
      	checkDefaultSuckerPassword(password);
      	
      	// The special user SUCKER_USER is used for creating internal connections that suck messages between nodes
      	
      	authenticated = suckerPassword.equals(password);
      }
      else
      {
      	authenticated = authenticationManager.isValid(principal, passwordChars, subject);
      }

      if (authenticated)
      {
         // Warning! This "taints" thread local. Make sure you pop it off the stack as soon as
         //          you're done with it.
         SecurityActions.pushSubjectContext(principal, passwordChars, subject);
         return subject;
      }
      else
      {
         throw new JMSSecurityException("User " + user + " is NOT authenticated");
      }
   }

   public boolean authorize(String user, Set rolePrincipals, CheckType checkType)
   {
      if (trace) { log.trace("authorizing user " + user + " for role(s) " + rolePrincipals.toString()); }
      
      if (SUCKER_USER.equals(user))
      {
      	//The special user SUCKER_USER is used for creating internal connections that suck messages between nodes
      	//It has automatic read/write access to all destinations
      	return (checkType.equals(CheckType.READ) || checkType.equals(CheckType.WRITE));
      }

      Principal principal = user == null ? null : new SimplePrincipal(user);
	
      boolean hasRole = realmMapping.doesUserHaveRole(principal, rolePrincipals);

      if (trace) { log.trace("user " + user + (hasRole ? " is " : " is NOT ") + "authorized"); }

      return hasRole;     
   }
   
   // Public --------------------------------------------------------
   
   public void setSuckerPassword(String password)
   {   	   	
   	checkDefaultSuckerPassword(password);
   	   	
   	this.suckerPassword = password;
   }
   
   public void start() throws NamingException
   {
      if (trace) { log.trace("initializing SecurityMetadataStore"); }

      // Get the JBoss security manager from JNDI
      InitialContext ic = new InitialContext();

      try
      {
         Object mgr = ic.lookup(messagingServer.getConfiguration().getSecurityDomain());

         log.debug("JaasSecurityManager is " + mgr);

         authenticationManager = (AuthenticationManager)mgr;
         realmMapping = (RealmMapping)mgr;

         log.trace("SecurityMetadataStore initialized");
      }
      catch (NamingException e)
      {
         // Apparently there is no security context, try adding java:/jaas
         log.warn("Failed to lookup securityDomain " + messagingServer.getConfiguration().getSecurityDomain(), e);

         if (!messagingServer.getConfiguration().getSecurityDomain().startsWith("java:/jaas/"))
         {
            authenticationManager =
               (SubjectSecurityManager)ic.lookup("java:/jaas/" + messagingServer.getConfiguration().getSecurityDomain());
         }
         else
         {
            throw e;
         }
      }
      finally
      {
         ic.close();
      }
   }

   public void stop() throws Exception
   {
   }



   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private void checkDefaultSuckerPassword(String password)
   {
   	// Sanity check
   	if (DEFAULT_SUCKER_USER_PASSWORD.equals(password))
   	{
   		log.warn("WARNING! POTENTIAL SECURITY RISK. It has been detected that the MessageSucker component " +
   				   "which sucks messages from one node to another has not had its password changed from the installation default. " +
   				   "Please see the JBoss Messaging user guide for instructions on how to do this.");
   	}
   }

   // Inner class ---------------------------------------------------      

}
