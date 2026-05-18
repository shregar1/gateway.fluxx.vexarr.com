use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Project: Top-level organizational unit for a collection of environments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub environments: HashMap<String, Environment>,
}

/// Environment: Scoped deployment context (e.g., Staging, Production).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: String,
    pub name: String,
    pub services: HashMap<String, Service>,
}

/// Service: Individual application or network component being monitored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    pub name: String,
    pub protocol_configs: Vec<String>, // List of associated capturer names
}

/// WorkspaceManager: Unified state for managing the Fluxx project hierarchy.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WorkspaceManager {
    pub projects: HashMap<String, Project>,
}

impl WorkspaceManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_project(&mut self, project: Project) {
        self.projects.insert(project.id.clone(), project);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 33: Service creation
    #[test]
    fn test_service_creation() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test Service".to_string(),
            protocol_configs: vec!["http".to_string(), "ws".to_string()],
        };
        assert_eq!(service.id, "svc-1");
        assert_eq!(service.protocol_configs.len(), 2);
    }

    // Test 34: Environment creation
    #[test]
    fn test_environment_creation() {
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        };
        env.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string()],
        });
        assert_eq!(env.services.len(), 1);
    }

    // Test 35: Project creation
    #[test]
    fn test_project_creation() {
        let project = Project {
            id: "proj-1".to_string(),
            name: "My Project".to_string(),
            environments: HashMap::new(),
        };
        assert_eq!(project.id, "proj-1");
        assert!(project.environments.is_empty());
    }

    // Test 36: WorkspaceManager new
    #[test]
    fn test_workspace_manager_new() {
        let manager = WorkspaceManager::new();
        assert!(manager.projects.is_empty());
    }

    // Test 37: WorkspaceManager default
    #[test]
    fn test_workspace_manager_default() {
        let manager: WorkspaceManager = Default::default();
        assert!(manager.projects.is_empty());
    }

    // Test 38: Add project to workspace
    #[test]
    fn test_workspace_add_project() {
        let mut manager = WorkspaceManager::new();
        let project = Project {
            id: "proj-1".to_string(),
            name: "Test Project".to_string(),
            environments: HashMap::new(),
        };
        manager.add_project(project);
        assert_eq!(manager.projects.len(), 1);
        assert!(manager.projects.contains_key("proj-1"));
    }

    // Test 39: Add multiple projects
    #[test]
    fn test_workspace_add_multiple_projects() {
        let mut manager = WorkspaceManager::new();
        manager.add_project(Project { id: "p1".to_string(), name: "P1".to_string(), environments: HashMap::new() });
        manager.add_project(Project { id: "p2".to_string(), name: "P2".to_string(), environments: HashMap::new() });
        manager.add_project(Project { id: "p3".to_string(), name: "P3".to_string(), environments: HashMap::new() });
        assert_eq!(manager.projects.len(), 3);
    }

    // Test 40: Project serialization
    #[test]
    fn test_project_serialization() {
        let project = Project {
            id: "proj-1".to_string(),
            name: "Test".to_string(),
            environments: HashMap::new(),
        };
        let json = serde_json::to_string(&project).unwrap();
        assert!(json.contains("proj-1"));
        assert!(json.contains("Test"));
    }

    // Test 301: Service creation with empty protocol_configs
    #[test]
    fn test_service_empty_protocol_configs() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test Service".to_string(),
            protocol_configs: vec![],
        };
        assert!(service.protocol_configs.is_empty());
    }

    // Test 302: Service with multiple protocol configs
    #[test]
    fn test_service_multiple_protocols() {
        let configs = vec!["http".to_string(), "grpc".to_string(), "ws".to_string()];
        let service = Service {
            id: "svc-1".to_string(),
            name: "Multi".to_string(),
            protocol_configs: configs,
        };
        assert_eq!(service.protocol_configs.len(), 3);
    }

    // Test 303: Service clone
    #[test]
    fn test_service_clone() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test".to_string(),
            protocol_configs: vec!["http".to_string()],
        };
        let cloned = service.clone();
        assert_eq!(service.id, cloned.id);
        assert_eq!(service.protocol_configs, cloned.protocol_configs);
    }

    // Test 304: Service debug format
    #[test]
    fn test_service_debug() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test".to_string(),
            protocol_configs: vec!["http".to_string()],
        };
        let debug_str = format!("{:?}", service);
        assert!(debug_str.contains("svc-1"));
    }

    // Test 305: Service serialization
    #[test]
    fn test_service_serialization() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test".to_string(),
            protocol_configs: vec!["http".to_string(), "grpc".to_string()],
        };
        let json = serde_json::to_string(&service).unwrap();
        assert!(json.contains("svc-1"));
        assert!(json.contains("protocol_configs"));
    }

    // Test 306: Service deserialization
    #[test]
    fn test_service_deserialization() {
        let json = r#"{"id":"svc-1","name":"Test","protocol_configs":["http","ws"]}"#;
        let service: Service = serde_json::from_str(json).unwrap();
        assert_eq!(service.id, "svc-1");
        assert_eq!(service.protocol_configs.len(), 2);
    }

    // Test 307: Service round-trip
    #[test]
    fn test_service_round_trip() {
        let original = Service {
            id: "svc-1".to_string(),
            name: "Test".to_string(),
            protocol_configs: vec!["http".to_string()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Service = serde_json::from_str(&json).unwrap();
        assert_eq!(original.id, deserialized.id);
    }

    // Test 308: Service with empty id and name
    #[test]
    fn test_service_empty_fields() {
        let service = Service {
            id: "".to_string(),
            name: "".to_string(),
            protocol_configs: vec![],
        };
        assert!(service.id.is_empty());
        assert!(service.name.is_empty());
    }

    // Test 309: Service with unicode
    #[test]
    fn test_service_unicode() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "服务 🚀".to_string(),
            protocol_configs: vec!["http".to_string()],
        };
        assert_eq!(service.name, "服务 🚀");
    }

    // Test 310: Environment creation with services
    #[test]
    fn test_environment_with_services() {
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        };
        env.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string()],
        });
        assert_eq!(env.services.len(), 1);
    }

    // Test 311: Environment empty services
    #[test]
    fn test_environment_empty_services() {
        let env = Environment {
            id: "env-1".to_string(),
            name: "empty".to_string(),
            services: HashMap::new(),
        };
        assert!(env.services.is_empty());
    }

    // Test 312: Environment clone
    #[test]
    fn test_environment_clone() {
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "prod".to_string(),
            services: HashMap::new(),
        };
        env.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string()],
        });
        let cloned = env.clone();
        assert_eq!(env.services.len(), cloned.services.len());
    }

    // Test 313: Environment debug format
    #[test]
    fn test_environment_debug() {
        let env = Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        };
        let debug_str = format!("{:?}", env);
        assert!(debug_str.contains("env-1"));
    }

    // Test 314: Environment serialization
    #[test]
    fn test_environment_serialization() {
        let env = Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        };
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains("env-1"));
        assert!(json.contains("staging"));
    }

    // Test 315: Environment deserialization
    #[test]
    fn test_environment_deserialization() {
        let json = r#"{"id":"env-1","name":"prod","services":{"svc-1":{"id":"svc-1","name":"API","protocol_configs":["http"]}}}"#;
        let env: Environment = serde_json::from_str(json).unwrap();
        assert_eq!(env.id, "env-1");
        assert_eq!(env.services.len(), 1);
    }

    // Test 316: Environment round-trip
    #[test]
    fn test_environment_round_trip() {
        let mut original = Environment {
            id: "env-1".to_string(),
            name: "test".to_string(),
            services: HashMap::new(),
        };
        original.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string()],
        });
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Environment = serde_json::from_str(&json).unwrap();
        assert_eq!(original.services.len(), deserialized.services.len());
    }

    // Test 317: Project with environments
    #[test]
    fn test_project_with_environments() {
        let mut project = Project {
            id: "proj-1".to_string(),
            name: "My Project".to_string(),
            environments: HashMap::new(),
        };
        project.environments.insert("env-1".to_string(), Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        });
        assert_eq!(project.environments.len(), 1);
    }

    // Test 318: Project empty environments
    #[test]
    fn test_project_empty_environments() {
        let project = Project {
            id: "proj-1".to_string(),
            name: "Empty".to_string(),
            environments: HashMap::new(),
        };
        assert!(project.environments.is_empty());
    }

    // Test 319: Project clone
    #[test]
    fn test_project_clone() {
        let project = Project {
            id: "proj-1".to_string(),
            name: "Test".to_string(),
            environments: HashMap::new(),
        };
        let cloned = project.clone();
        assert_eq!(project.id, cloned.id);
    }

    // Test 320: Project debug format
    #[test]
    fn test_project_debug() {
        let project = Project {
            id: "proj-1".to_string(),
            name: "Test".to_string(),
            environments: HashMap::new(),
        };
        let debug_str = format!("{:?}", project);
        assert!(debug_str.contains("proj-1"));
    }

    // Test 321: Project deserialization
    #[test]
    fn test_project_deserialization() {
        let json = r#"{"id":"proj-1","name":"Test","environments":{}}"#;
        let project: Project = serde_json::from_str(json).unwrap();
        assert_eq!(project.id, "proj-1");
    }

    // Test 322: Project round-trip
    #[test]
    fn test_project_round_trip() {
        let original = Project {
            id: "proj-1".to_string(),
            name: "Test".to_string(),
            environments: HashMap::new(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Project = serde_json::from_str(&json).unwrap();
        assert_eq!(original.id, deserialized.id);
    }

    // Test 323: WorkspaceManager add duplicate project
    #[test]
    fn test_workspace_add_duplicate_project() {
        let mut manager = WorkspaceManager::new();
        manager.add_project(Project { id: "p1".to_string(), name: "P1".to_string(), environments: HashMap::new() });
        manager.add_project(Project { id: "p1".to_string(), name: "P1-New".to_string(), environments: HashMap::new() });
        assert_eq!(manager.projects.len(), 1);
    }

    // Test 324: WorkspaceManager with many projects
    #[test]
    fn test_workspace_many_projects() {
        let mut manager = WorkspaceManager::new();
        for i in 0..100 {
            manager.add_project(Project {
                id: format!("proj-{}", i),
                name: format!("Project {}", i),
                environments: HashMap::new(),
            });
        }
        assert_eq!(manager.projects.len(), 100);
    }

    // Test 325: WorkspaceManager clone
    #[test]
    fn test_workspace_manager_clone() {
        let mut manager = WorkspaceManager::new();
        manager.add_project(Project { id: "p1".to_string(), name: "P1".to_string(), environments: HashMap::new() });
        let cloned = manager.clone();
        assert_eq!(manager.projects.len(), cloned.projects.len());
    }

    // Test 326: WorkspaceManager debug format
    #[test]
    fn test_workspace_manager_debug() {
        let manager = WorkspaceManager::new();
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("WorkspaceManager"));
    }

    // Test 327: WorkspaceManager serialization
    #[test]
    fn test_workspace_manager_serialization() {
        let mut manager = WorkspaceManager::new();
        manager.add_project(Project { id: "p1".to_string(), name: "P1".to_string(), environments: HashMap::new() });
        let json = serde_json::to_string(&manager).unwrap();
        assert!(json.contains("p1"));
    }

    // Test 328: WorkspaceManager deserialization
    #[test]
    fn test_workspace_manager_deserialization() {
        let json = r#"{"projects":{"p1":{"id":"p1","name":"P1","environments":{}}}}"#;
        let manager: WorkspaceManager = serde_json::from_str(json).unwrap();
        assert_eq!(manager.projects.len(), 1);
    }

    // Test 329: WorkspaceManager round-trip
    #[test]
    fn test_workspace_manager_round_trip() {
        let mut original = WorkspaceManager::new();
        original.add_project(Project { id: "p1".to_string(), name: "P1".to_string(), environments: HashMap::new() });
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: WorkspaceManager = serde_json::from_str(&json).unwrap();
        assert_eq!(original.projects.len(), deserialized.projects.len());
    }

    // Test 330: Complex hierarchy
    #[test]
    fn test_complex_hierarchy() {
        let mut manager = WorkspaceManager::new();
        let mut project = Project {
            id: "proj-1".to_string(),
            name: "Project".to_string(),
            environments: HashMap::new(),
        };
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "staging".to_string(),
            services: HashMap::new(),
        };
        env.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string()],
        });
        project.environments.insert("env-1".to_string(), env);
        manager.add_project(project);
        assert_eq!(manager.projects.len(), 1);
        assert_eq!(manager.projects["proj-1"].environments.len(), 1);
        assert_eq!(manager.projects["proj-1"].environments["env-1"].services.len(), 1);
    }

    // Test 331: Service with many protocol configs
    #[test]
    fn test_service_many_protocols() {
        let configs: Vec<String> = (0..50).map(|i| format!("proto-{}", i)).collect();
        let service = Service {
            id: "svc-1".to_string(),
            name: "Multi".to_string(),
            protocol_configs: configs,
        };
        assert_eq!(service.protocol_configs.len(), 50);
    }

    // Test 332: Environment with many services
    #[test]
    fn test_environment_many_services() {
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "prod".to_string(),
            services: HashMap::new(),
        };
        for i in 0..100 {
            env.services.insert(format!("svc-{}", i), Service {
                id: format!("svc-{}", i),
                name: format!("Service {}", i),
                protocol_configs: vec!["http".to_string()],
            });
        }
        assert_eq!(env.services.len(), 100);
    }

    // Test 333: Project with many environments
    #[test]
    fn test_project_many_environments() {
        let mut project = Project {
            id: "proj-1".to_string(),
            name: "Big".to_string(),
            environments: HashMap::new(),
        };
        for i in 0..50 {
            project.environments.insert(format!("env-{}", i), Environment {
                id: format!("env-{}", i),
                name: format!("Env {}", i),
                services: HashMap::new(),
            });
        }
        assert_eq!(project.environments.len(), 50);
    }

    // Test 334: Full workspace serialization
    #[test]
    fn test_full_workspace_serialization() {
        let mut manager = WorkspaceManager::new();
        let mut project = Project {
            id: "proj-1".to_string(),
            name: "Project".to_string(),
            environments: HashMap::new(),
        };
        let mut env = Environment {
            id: "env-1".to_string(),
            name: "prod".to_string(),
            services: HashMap::new(),
        };
        env.services.insert("svc-1".to_string(), Service {
            id: "svc-1".to_string(),
            name: "API".to_string(),
            protocol_configs: vec!["http".to_string(), "grpc".to_string()],
        });
        project.environments.insert("env-1".to_string(), env);
        manager.add_project(project);
        let json = serde_json::to_string(&manager).unwrap();
        assert!(json.contains("proj-1"));
        assert!(json.contains("env-1"));
        assert!(json.contains("svc-1"));
    }

    // Test 335: Service serialization preserves order
    #[test]
    fn test_service_protocol_order() {
        let service = Service {
            id: "svc-1".to_string(),
            name: "Test".to_string(),
            protocol_configs: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let json = serde_json::to_string(&service).unwrap();
        let deserialized: Service = serde_json::from_str(&json).unwrap();
        assert_eq!(service.protocol_configs, deserialized.protocol_configs);
    }

    // Test 336: Empty workspace manager default
    #[test]
    fn test_workspace_manager_default_empty() {
        let manager: WorkspaceManager = Default::default();
        assert!(manager.projects.is_empty());
    }

    // Test 337: Service partial equality
    #[test]
    fn test_service_partial_eq() {
        let s1 = Service { id: "s1".to_string(), name: "S".to_string(), protocol_configs: vec!["http".to_string()] };
        let s2 = Service { id: "s1".to_string(), name: "S".to_string(), protocol_configs: vec!["http".to_string()] };
        assert_eq!(s1.id, s2.id);
    }

    // Test 338: Environment partial equality
    #[test]
    fn test_environment_partial_eq() {
        let e1 = Environment { id: "e1".to_string(), name: "E".to_string(), services: HashMap::new() };
        let e2 = Environment { id: "e1".to_string(), name: "E".to_string(), services: HashMap::new() };
        assert_eq!(e1.id, e2.id);
    }

    // Test 339: Project partial equality
    #[test]
    fn test_project_partial_eq() {
        let p1 = Project { id: "p1".to_string(), name: "P".to_string(), environments: HashMap::new() };
        let p2 = Project { id: "p1".to_string(), name: "P".to_string(), environments: HashMap::new() };
        assert_eq!(p1.id, p2.id);
    }

    // Test 340: WorkspaceManager is Send + Sync
    #[test]
    fn test_topology_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WorkspaceManager>();
        assert_send_sync::<Project>();
        assert_send_sync::<Environment>();
        assert_send_sync::<Service>();
    }
}
